from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import duckdb

from aster import lake
from aster.faults import hit
from aster.obs import log, span

ROOT = Path(__file__).resolve().parents[2]
SILVER_SQL = ROOT / "sql/silver"
GOLD_SQL = ROOT / "sql/gold"
SILVER_DATASETS = ("customers", "subscriptions", "payments", "service_events")
DIMENSIONS = ("dim_customer", "dim_subscription")
FACTS = ("fact_payments", "fact_service_events")
FACT_DATE_COLUMN = {"fact_payments": "paid_at", "fact_service_events": "event_time"}
FACT_PARTITION = {"fact_payments": "payment_month", "fact_service_events": "event_month"}
TREND_LOOKBACK_DAYS = 6
TREND_SEED_TABLE = "revenue_trend_seed"
PRIMARY_KEYS = {
    "customers": "customer_id",
    "subscriptions": "subscription_id",
    "payments": "payment_id",
    "service_events": "event_id",
}

RAW_EMPTY_SCHEMAS = {
    "customers": "customer_id VARCHAR, name VARCHAR, region VARCHAR, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, filename VARCHAR",
    "subscriptions": "subscription_id VARCHAR, customer_id VARCHAR, plan_name VARCHAR, monthly_price DECIMAL(12,2), status VARCHAR, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, filename VARCHAR",
    "payments": "payment_id VARCHAR, subscription_id VARCHAR, amount DECIMAL(12,2), currency VARCHAR, status VARCHAR, paid_at TIMESTAMPTZ, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, filename VARCHAR",
    "service_events": "event_id VARCHAR, customer_id VARCHAR, event_type VARCHAR, event_time TIMESTAMPTZ, ingested_at TIMESTAMPTZ, duration_ms INTEGER, status_code INTEGER, payload VARCHAR, filename VARCHAR",
}


@dataclass(frozen=True)
class SilverResult:
    run_id: str
    datasets: dict
    pointer: dict


@dataclass(frozen=True)
class GoldResult:
    run_id: str
    tables: dict
    trend_seed: dict
    pointer: dict


def connect(config):
    connection = duckdb.connect()
    connection.execute("LOAD httpfs")
    connection.execute(f"SET s3_endpoint='{config.s3_host()}'")
    connection.execute("SET s3_use_ssl=false")
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_access_key_id='test'")
    connection.execute("SET s3_secret_access_key='test'")
    connection.execute(f"SET s3_region='{config.region}'")
    connection.execute("SET timezone='UTC'")
    return connection


def sql_list(values):
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _raw_view(connection, config, dataset, manifests):
    uris = [
        lake.s3_uri(config.raw_bucket, item["key"])
        for manifest in manifests
        for item in manifest["objects"]
    ]
    if uris:
        connection.execute(
            f"CREATE OR REPLACE VIEW raw_{dataset} AS SELECT * FROM read_parquet([{sql_list(uris)}], filename=true, union_by_name=true)"
        )
    else:
        connection.execute(
            f"CREATE OR REPLACE TABLE raw_{dataset} ({RAW_EMPTY_SCHEMAS[dataset]})"
        )


def _copy_to_s3(connection, query, target, partition_by=None):
    options = "FORMAT PARQUET, COMPRESSION ZSTD"
    if partition_by:
        options += f", PARTITION_BY ({partition_by}), OVERWRITE_OR_IGNORE"
    connection.execute(f"COPY ({query}) TO '{target}' ({options})")


def _row_count(connection, uris):
    if not uris:
        return 0
    return connection.execute(
        "SELECT count(*) FROM read_parquet(?::VARCHAR[], union_by_name=true)", [uris]
    ).fetchone()[0]


def _publish_objects(
    s3, bucket, connection, prefix, query, row_count, partition_by=None, filename="data.parquet"
):
    partitioned = bool(partition_by) and row_count > 0
    target = prefix if partitioned else f"{prefix}/{filename}"
    _copy_to_s3(
        connection,
        query,
        lake.s3_uri(bucket, target),
        partition_by if partitioned else None,
    )
    listing_prefix = f"{prefix}/" if partitioned else target
    keys = sorted(
        key
        for key in lake.list_keys(s3, bucket, listing_prefix)
        if key.endswith(".parquet")
    )
    if not keys:
        raise ValueError(f"No parquet objects written under {prefix}")
    return keys


def _verify_objects(s3, connection, bucket, keys, expected_rows, dataset):
    uris = [lake.s3_uri(bucket, key) for key in keys]
    observed = _row_count(connection, uris)
    if observed != expected_rows:
        raise ValueError(
            f"Row count mismatch for {dataset}: wrote {expected_rows}, read {observed}"
        )
    return {key: lake.sha256_hex(lake.read_bytes(s3, bucket, key)) for key in keys}


def build_silver(s3, config, run_id):
    with span("transform_silver", run_id=run_id):
        connection = connect(config)
        datasets = {}
        for dataset in SILVER_DATASETS:
            manifests = lake.raw_manifests(s3, config.raw_bucket, dataset)
            _raw_view(connection, config, dataset, manifests)
            distinct_raw_rows = connection.execute(
                f"SELECT count(DISTINCT {PRIMARY_KEYS[dataset]}) FROM raw_{dataset}"
            ).fetchone()[0]
            statement = (SILVER_SQL / f"{dataset}.sql").read_text()
            connection.execute(
                f"CREATE OR REPLACE TEMP TABLE staged_{dataset} AS {statement}",
                [],
            )
            connection.execute(
                f"CREATE OR REPLACE TEMP TABLE silver_{dataset} AS SELECT * EXCLUDE (_reject_reason) FROM staged_{dataset} WHERE _reject_reason IS NULL"
            )
            connection.execute(
                f"CREATE OR REPLACE TEMP TABLE rejected_{dataset} AS SELECT * FROM staged_{dataset} WHERE _reject_reason IS NOT NULL"
            )
            row_count = connection.execute(
                f"SELECT count(*) FROM silver_{dataset}"
            ).fetchone()[0]
            rejected_count = connection.execute(
                f"SELECT count(*) FROM rejected_{dataset}"
            ).fetchone()[0]
            base = f"silver/{dataset}/run={run_id}"
            object_keys = _publish_objects(
                s3,
                config.silver_bucket,
                connection,
                base,
                f"SELECT * FROM silver_{dataset}",
                row_count,
            )
            rejected_keys = _publish_objects(
                s3,
                config.silver_bucket,
                connection,
                base,
                f"SELECT * FROM rejected_{dataset}",
                rejected_count,
                None,
                "rejected.parquet",
            )
            checksums = _verify_objects(
                s3, connection, config.silver_bucket, object_keys, row_count, dataset
            )
            checksums.update(
                _verify_objects(
                    s3,
                    connection,
                    config.silver_bucket,
                    rejected_keys,
                    rejected_count,
                    f"{dataset} rejections",
                )
            )
            datasets[dataset] = {
                "objects": object_keys,
                "rejected_objects": rejected_keys,
                "row_count": row_count,
                "rejected_count": rejected_count,
                "raw_manifest_count": len(manifests),
                "raw_row_count": sum(item["record_count"] for item in manifests),
                "distinct_raw_rows": distinct_raw_rows,
                "checksums": checksums,
            }
            lake.put_json(
                s3,
                config.silver_bucket,
                f"{base}/manifest.json",
                {
                    "run_id": run_id,
                    "dataset": dataset,
                    "row_count": row_count,
                    "rejected_count": rejected_count,
                    "objects": object_keys,
                    "rejected_objects": rejected_keys,
                    "raw_manifest_count": len(manifests),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            log(
                "silver_dataset_written",
                dataset=dataset,
                rows=row_count,
                rejected=rejected_count,
                raw_manifests=len(manifests),
            )
        pointer = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "datasets": datasets,
        }
        lake.put_json(s3, config.silver_bucket, "silver/_committed.json", pointer)
        return SilverResult(run_id=run_id, datasets=datasets, pointer=pointer)


def _silver_tables(connection, config, pointer, window):
    for dataset in SILVER_DATASETS:
        objects = pointer["datasets"][dataset]["objects"]
        uris = [lake.s3_uri(config.silver_bucket, key) for key in objects]
        connection.execute(
            f"CREATE OR REPLACE VIEW source_{dataset} AS SELECT * FROM read_parquet([{sql_list(uris)}], union_by_name=true)"
        )
    windowed = {"payments": "paid_at", "service_events": "event_time"} if window else {}
    start, end = window_bounds(*window) if window else (None, None)
    for dataset in SILVER_DATASETS:
        if dataset in windowed:
            connection.execute(
                f"CREATE OR REPLACE TEMP TABLE silver_{dataset} AS "
                f"SELECT * FROM source_{dataset} "
                f"WHERE {windowed[dataset]} >= TIMESTAMP '{start.isoformat()}' "
                f"AND {windowed[dataset]} < TIMESTAMP '{end.isoformat()}'"
            )
        else:
            connection.execute(
                f"CREATE OR REPLACE TEMP TABLE silver_{dataset} AS SELECT * FROM source_{dataset}"
            )


def _trend_seed_query(window):
    if window:
        start = window[0].isoformat()
        end = window[1].isoformat()
        lookback_start = (window[0] - timedelta(days=TREND_LOOKBACK_DAYS)).isoformat()
        range_start = f"greatest(DATE '{lookback_start}', bounds.min_day)"
        range_end = f"least(DATE '{end}', bounds.max_day)"
        window_start = f"greatest(DATE '{start}', bounds.min_day)"
    else:
        range_start = range_end = window_start = None
    bounds = (
        "SELECT CAST(min(paid_at) AS DATE) AS min_day, "
        "CAST(max(paid_at) AS DATE) AS max_day FROM source_payments "
        "WHERE status = 'succeeded' AND paid_at IS NOT NULL"
    )
    range_cte = (
        "SELECT "
        + (f"{range_start} AS range_start, {range_end} AS range_end, " if window else "bounds.min_day AS range_start, bounds.max_day AS range_end, ")
        + (f"{window_start} AS window_start " if window else "bounds.min_day AS window_start ")
        + "FROM bounds"
    )
    return (
        "WITH bounds AS ("
        + bounds
        + "), date_range AS ("
        + range_cte
        + "), calendar AS ("
        "SELECT unnest(generate_series(range_start, range_end, INTERVAL 1 DAY))::DATE AS calendar_date, "
        "window_start FROM date_range WHERE range_start IS NOT NULL AND range_end IS NOT NULL"
        "), prior AS ("
        "SELECT CAST(s.paid_at AS DATE) AS calendar_date, sum(s.amount) AS prior_revenue "
        "FROM source_payments s, date_range "
        "WHERE s.status = 'succeeded' AND s.paid_at IS NOT NULL "
        "AND CAST(s.paid_at AS DATE) < date_range.window_start "
        "GROUP BY 1"
        ") "
        "SELECT calendar.calendar_date, "
        "calendar.calendar_date < calendar.window_start AS is_lookback, "
        "coalesce(prior.prior_revenue, 0) AS prior_revenue "
        "FROM calendar LEFT JOIN prior USING (calendar_date) "
        "ORDER BY calendar.calendar_date"
    )


def _publish_trend_seed(s3, config, connection, run_id, window):
    connection.execute(
        f"CREATE OR REPLACE TEMP TABLE {TREND_SEED_TABLE} AS {_trend_seed_query(window)}"
    )
    row_count = connection.execute(
        f"SELECT count(*) FROM {TREND_SEED_TABLE}"
    ).fetchone()[0]
    prefix = f"gold/{TREND_SEED_TABLE}/run={run_id}"
    keys = _publish_objects(
        s3,
        config.gold_bucket,
        connection,
        prefix,
        f"SELECT * FROM {TREND_SEED_TABLE}",
        row_count,
    )
    checksums = _verify_objects(
        s3, connection, config.gold_bucket, keys, row_count, TREND_SEED_TABLE
    )
    log(
        "gold_trend_seed_written",
        table=TREND_SEED_TABLE,
        rows=row_count,
        objects=len(keys),
    )
    return {"objects": keys, "row_count": row_count, "checksums": checksums}


def build_gold(s3, config, run_id, silver_pointer, window=None):
    with span("build_gold", run_id=run_id):
        connection = connect(config)
        _silver_tables(connection, config, silver_pointer, window)
        tables = {}
        for table in DIMENSIONS + FACTS:
            connection.execute(
                f"CREATE OR REPLACE TEMP TABLE {table} AS {(GOLD_SQL / f'{table}.sql').read_text()}"
            )
            row_count = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            prefix = f"gold/{table}/run={run_id}"
            keys = _publish_objects(
                s3,
                config.gold_bucket,
                connection,
                prefix,
                f"SELECT * FROM {table}",
                row_count,
                FACT_PARTITION.get(table),
            )
            checksums = _verify_objects(
                s3, connection, config.gold_bucket, keys, row_count, table
            )
            tables[table] = {
                "objects": keys,
                "row_count": row_count,
                "checksums": checksums,
            }
            log("gold_table_written", table=table, rows=row_count, objects=len(keys))
        trend_seed = _publish_trend_seed(s3, config, connection, run_id, window)
        hit("gold-before-publish")
        pointer = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "silver_run_id": silver_pointer["run_id"],
            "window": [window[0].isoformat(), window[1].isoformat()]
            if window
            else None,
            "trend_lookback_days": TREND_LOOKBACK_DAYS,
            "trend_seed": trend_seed,
            "tables": tables,
        }
        if window:
            lake.put_json(
                s3,
                config.gold_bucket,
                f"gold/backfills/{run_id}/manifest.json",
                pointer,
            )
        else:
            lake.put_json(s3, config.gold_bucket, "gold/_committed.json", pointer)
        return GoldResult(
            run_id=run_id, tables=tables, trend_seed=trend_seed, pointer=pointer
        )


def window_bounds(start, end):
    return (
        datetime.combine(start, time.min),
        datetime.combine(end, time.min) + timedelta(days=1),
    )
