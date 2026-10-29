import json

import pytest

from aster import lake, metrics, pipeline
from aster.ingestion import ingest
from aster.source import migrate, seed
from aster.transform import sql_list
from conftest import run_aster

pytestmark = [pytest.mark.integration, pytest.mark.failure]

DATASETS = ("customers", "subscriptions", "payments", "service_events")


def seed_environment(config):
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)


def metric_rows(config, s3, pointer=None):
    connection = metrics.duckdb_connect(config)
    try:
        metrics.create_gold_views(connection, s3, config, pointer)
        return {
            name: [
                [str(value) for value in row.values()]
                for row in metrics.run_metric(connection, name)
            ]
            for name in metrics.METRICS
        }
    finally:
        connection.close()


def raw_keys(s3, bucket):
    return sorted(lake.list_keys(s3, bucket, ""))


def checkpoint(config, dataset):
    with pipeline.connect_postgres(config) as connection:
        return connection.execute(
            "SELECT cursor_time, cursor_id FROM extraction_checkpoints WHERE dataset = %s",
            (dataset,),
        ).fetchone()


def test_scenario_a_interruption_after_data_write(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seed_environment(config)
    killed = run_aster(
        config,
        "--run-id",
        "fault-a1",
        "extract",
        extra_env={"ASTER_FAULT_POINT": "ingest-after-data"},
        check=False,
    )
    assert killed.returncode == -9
    manifests = lake.raw_manifests(s3, config.raw_bucket, "customers")
    assert manifests == []
    orphaned = lake.list_keys(s3, lake_env.buckets["raw"], "customers/")
    assert len(orphaned) == 1 and orphaned[0].endswith("data.parquet")
    ingestion = run_aster(config, "--run-id", "fault-a2", "extract")
    assert json.loads(ingestion.stdout)[
        "rows"
    ]["customers"] == 100
    manifests = lake.raw_manifests(s3, config.raw_bucket, "customers")
    assert len(manifests) == 1
    assert manifests[0]["record_count"] == 100
    keys = lake.list_keys(s3, lake_env.buckets["raw"], manifests[0]["objects"][0]["key"])
    assert keys == [manifests[0]["objects"][0]["key"]]
    pipeline.run_pipeline(config, "fault-a3")
    silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    assert silver["datasets"]["customers"]["row_count"] == 100


def test_scenario_a_interruption_after_manifest_commit(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seed_environment(config)
    killed = run_aster(
        config,
        "--run-id",
        "fault-a4",
        "extract",
        extra_env={"ASTER_FAULT_POINT": "ingest-after-manifest"},
        check=False,
    )
    assert killed.returncode == -9
    manifests = lake.raw_manifests(s3, config.raw_bucket, "customers")
    assert len(manifests) == 1
    assert checkpoint(config, "customers") is None
    before = [
        key
        for key in raw_keys(s3, lake_env.buckets["raw"])
        if key.startswith("customers/")
    ]
    replay = run_aster(config, "--run-id", "fault-a5", "extract")
    assert json.loads(replay.stdout)["rows"]["customers"] == 100
    after = [
        key
        for key in raw_keys(s3, lake_env.buckets["raw"])
        if key.startswith("customers/")
    ]
    assert after == before
    assert len(lake.raw_manifests(s3, config.raw_bucket, "customers")) == 1
    pipeline.run_pipeline(config, "fault-a6")
    silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    assert silver["datasets"]["customers"]["row_count"] == 100


def test_scenario_b_duplicate_replay_preserves_results(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seed_environment(config)
    pipeline.run_pipeline(config, "replay-a")
    before = metric_rows(config, s3)
    before_keys = raw_keys(s3, lake_env.buckets["raw"])
    with pipeline.connect_postgres(config) as connection:
        connection.execute(
            "UPDATE extraction_checkpoints SET cursor_time = '0001-01-01 00:00:00+00', "
            "cursor_id = '00000000-0000-0000-0000-000000000000'"
        )
        replayed = ingest(connection, s3, config, "replay-b")
    assert replayed == {
        "customers": 100,
        "subscriptions": 100,
        "payments": 100,
        "service_events": 1000,
    }
    assert raw_keys(s3, lake_env.buckets["raw"]) == before_keys
    pipeline.run_pipeline(config, "replay-b")
    assert metric_rows(config, s3) == before
    silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    assert {name: info["row_count"] for name, info in silver["datasets"].items()} == {
        "customers": 100,
        "subscriptions": 100,
        "payments": 100,
        "service_events": 1000,
    }


def test_scenario_c_invalid_source_rows_are_quarantined(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seed_environment(config)
    pipeline.run_pipeline(config, "invalid-a")
    before = metric_rows(config, s3)
    before_silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    accepted_before = {
        name: info["row_count"] for name, info in before_silver["datasets"].items()
    }
    with pipeline.connect_postgres(config) as connection:
        customer = connection.execute(
            "INSERT INTO customers (customer_id, name, region, created_at, updated_at) "
            "VALUES (gen_random_uuid(), '', 'north', now(), now()) RETURNING customer_id"
        ).fetchone()[0]
        subscription = connection.execute(
            "INSERT INTO subscriptions (subscription_id, customer_id, plan_name, monthly_price, status, created_at, updated_at) "
            "VALUES (gen_random_uuid(), %s, 'bogus', -5.00, 'zombie', now(), now()) RETURNING subscription_id",
            (customer,),
        ).fetchone()[0]
        payment = connection.execute(
            "INSERT INTO payments (payment_id, subscription_id, amount, currency, status, paid_at, created_at, updated_at) "
            "VALUES (gen_random_uuid(), %s, -10.00, 'USD', 'succeeded', NULL, now(), now()) RETURNING payment_id",
            (subscription,),
        ).fetchone()[0]
        event = connection.execute(
            "INSERT INTO service_events (event_id, customer_id, event_type, event_time, ingested_at, duration_ms, status_code, payload) "
            "VALUES (gen_random_uuid(), %s, 'request', now(), now(), -5, 999, '{}'::jsonb) RETURNING event_id",
            (customer,),
        ).fetchone()[0]
    pipeline.run_pipeline(config, "invalid-b")
    silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    assert silver["datasets"]["customers"]["rejected_count"] >= 1
    assert silver["datasets"]["subscriptions"]["rejected_count"] >= 1
    assert silver["datasets"]["payments"]["rejected_count"] >= 1
    assert silver["datasets"]["service_events"]["rejected_count"] >= 1
    assert {
        name: info["row_count"] for name, info in silver["datasets"].items()
    } == accepted_before
    connection = metrics.duckdb_connect(config)
    try:
        rejected = [
            lake.s3_uri(
                config.silver_bucket,
                key,
            )
            for key in silver["datasets"]["service_events"]["rejected_objects"]
        ]
        rows = connection.execute(
            f"SELECT event_id, _reject_reason FROM read_parquet([{sql_list(rejected)}], union_by_name=true)"
        ).fetchall()
    finally:
        connection.close()
    reasons = {str(identifier): reason for identifier, reason in rows}
    assert reasons[str(event)] in {"negative duration_ms", "invalid status_code"}
    assert metric_rows(config, s3) == before
    assert str(payment) not in reasons


def test_scenario_d_interrupted_gold_keeps_previous_commit(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seed_environment(config)
    pipeline.run_pipeline(config, "gold-a")
    before_pointer = lake.committed_pointer(s3, config.gold_bucket, "gold")
    before = metric_rows(config, s3)
    killed = run_aster(
        config,
        "--run-id",
        "gold-b",
        "build-gold",
        extra_env={"ASTER_FAULT_POINT": "gold-before-publish"},
        check=False,
    )
    assert killed.returncode == -9
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == before_pointer
    assert metric_rows(config, s3) == before
    partial = lake.list_keys(s3, lake_env.buckets["gold"], "gold/dim_customer/run=gold-b/")
    assert len(partial) > 0
    rerun = run_aster(config, "--run-id", "gold-c", "build-gold")
    assert json.loads(rerun.stdout)["tables"]["dim_customer"] == 100
    pipeline.run_pipeline(config, "gold-d")
    assert metric_rows(config, s3) == before
    assert (
        lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"] == "gold-d"
    )


def windowed_reference_metrics(config, s3, start, end):
    connection = metrics.duckdb_connect(config)
    try:
        metrics.create_gold_views(connection, s3, config)
        connection.execute(
            "CREATE TABLE fact_payments_full AS SELECT * FROM fact_payments"
        )
        connection.execute(
            "CREATE TABLE fact_events_full AS SELECT * FROM fact_service_events"
        )
        connection.execute("DROP VIEW fact_payments")
        connection.execute("DROP VIEW fact_service_events")
        connection.execute(
            f"CREATE TABLE fact_payments AS SELECT * FROM fact_payments_full "
            f"WHERE payment_date BETWEEN DATE '{start}' AND DATE '{end}'"
        )
        connection.execute(
            f"CREATE TABLE fact_service_events AS SELECT * FROM fact_events_full "
            f"WHERE event_date BETWEEN DATE '{start}' AND DATE '{end}'"
        )
        return {
            name: [
                [str(value) for value in row.values()]
                for row in metrics.run_metric(connection, name)
            ]
            for name in metrics.METRICS
        }
    finally:
        connection.close()


def test_scenario_e_backfill_matches_clean_rebuild(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seed_environment(config)
    pipeline.run_pipeline(config, "backfill-a")
    start, end = "2025-03-01", "2025-03-31"
    reference = windowed_reference_metrics(config, s3, start, end)
    backfill = run_aster(
        config,
        "--run-id",
        "backfill-b",
        "backfill",
        "--start",
        start,
        "--end",
        end,
    )
    assert json.loads(backfill.stdout)["status"] == "ok"
    pointer = lake.get_json(
        s3, config.gold_bucket, "gold/backfills/backfill-b/manifest.json"
    )
    assert pointer["window"] == [start, end]
    observed = metric_rows(config, s3, pointer)
    daily_metrics = (
        "daily_successful_payment_revenue",
        "daily_active_customers",
        "daily_event_volume",
        "p95_service_duration_by_day",
    )
    for name in daily_metrics:
        assert observed[name] == reference[name], name
    assert observed["revenue_by_region"] == reference["revenue_by_region"]
    assert observed["revenue_by_plan"] == reference["revenue_by_plan"]
    observed_errors = {
        tuple(row[:2]): row for row in observed["service_error_rate_by_day_region"]
    }
    reference_errors = {
        tuple(row[:2]): row for row in reference["service_error_rate_by_day_region"]
    }
    assert observed_errors == reference_errors
    full_trend = {
        row[0]: row
        for row in metric_rows(config, s3)["seven_day_revenue_trend"]
        if start <= row[0] <= end
    }
    backfilled_trend = {
        row[0]: row
        for row in observed["seven_day_revenue_trend"]
        if row[0] >= "2025-03-07"
    }
    assert backfilled_trend == {
        day: rows for day, rows in full_trend.items() if day >= "2025-03-07"
    }
    assert lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"] == "backfill-a"
