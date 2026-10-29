import json
from dataclasses import dataclass

from aster import lake
from aster.transform import PRIMARY_KEYS, SILVER_DATASETS, connect, sql_list

REQUIRED_COLUMNS = {
    "customers": ("customer_id", "name", "region", "created_at", "updated_at"),
    "subscriptions": (
        "subscription_id",
        "customer_id",
        "plan_name",
        "monthly_price",
        "status",
    ),
    "payments": ("payment_id", "subscription_id", "amount", "currency", "status"),
    "service_events": ("event_id", "customer_id", "event_type", "event_time"),
}


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: dict

    def as_dict(self):
        return {"name": self.name, "ok": self.ok, "detail": self.detail}


class QualityError(RuntimeError):
    def __init__(self, checks):
        self.checks = checks
        failures = [check.as_dict() for check in checks if not check.ok]
        super().__init__(
            "Quality validation failed: " + json.dumps(failures, sort_keys=True)
        )


def check(name, ok, **detail):
    return Check(name=name, ok=bool(ok), detail=detail)


def require(checks):
    if not all(item.ok for item in checks):
        raise QualityError(checks)
    return checks


def _dataset_uris(bucket, keys):
    return [lake.s3_uri(bucket, key) for key in keys]


def validate_raw(s3, config, connection):
    checks = []
    warehouse = connect(config)
    for dataset in SILVER_DATASETS:
        manifests = lake.raw_manifests(s3, config.raw_bucket, dataset)
        checks.append(
            check(f"raw.{dataset}.manifests_present", bool(manifests), count=len(manifests))
        )
        for manifest in manifests:
            key = manifest["objects"][0]["key"]
            payload = lake.read_bytes(s3, config.raw_bucket, key)
            checks.append(
                check(
                    f"raw.{dataset}.checksum",
                    lake.sha256_hex(payload) == manifest["objects"][0]["sha256"],
                    batch_id=manifest["batch_id"],
                )
            )
            checks.append(
                check(
                    f"raw.{dataset}.status",
                    manifest["status"] == "complete",
                    batch_id=manifest["batch_id"],
                )
            )
        checks.append(
            check(
                f"raw.{dataset}.cursor_monotonic",
                all(
                    left["cursor"]["end"] <= right["cursor"]["end"]
                    for left, right in zip(manifests, manifests[1:])
                ),
                count=len(manifests),
            )
        )
        source_rows = connection.execute(
            f"SELECT count(*) FROM {dataset}"
        ).fetchone()[0]
        manifested = sum(item["record_count"] for item in manifests)
        uris = [
            lake.s3_uri(config.raw_bucket, item["key"])
            for manifest in manifests
            for item in manifest["objects"]
        ]
        distinct = warehouse.execute(
            f"SELECT count(DISTINCT {PRIMARY_KEYS[dataset]}) FROM read_parquet([{sql_list(uris)}], union_by_name=true)"
        ).fetchone()[0]
        if dataset == "service_events":
            checks.append(
                check(
                    f"raw.{dataset}.source_reconciliation",
                    source_rows == manifested,
                    source_rows=source_rows,
                    manifested_rows=manifested,
                )
            )
        else:
            checks.append(
                check(
                    f"raw.{dataset}.source_reconciliation",
                    distinct == source_rows and manifested >= distinct,
                    source_rows=source_rows,
                    manifested_rows=manifested,
                    distinct_keys=distinct,
                )
            )
    warehouse.close()
    return checks


def validate_silver(s3, config, pointer=None):
    pointer = pointer or lake.committed_pointer(s3, config.silver_bucket, "silver")
    checks = [check("silver.committed_pointer", pointer is not None)]
    if pointer is None:
        return checks
    connection = connect(config)
    for dataset in SILVER_DATASETS:
        info = pointer["datasets"][dataset]
        uris = _dataset_uris(config.silver_bucket, info["objects"])
        rejected = _dataset_uris(config.silver_bucket, info["rejected_objects"])
        observed = connection.execute(
            "SELECT count(*) FROM read_parquet(?::VARCHAR[], union_by_name=true)",
            [uris],
        ).fetchone()[0]
        checks.append(
            check(
                f"silver.{dataset}.row_count",
                observed == info["row_count"],
                expected=info["row_count"],
                observed=observed,
            )
        )
        for key, digest in info["checksums"].items():
            checks.append(
                check(
                    f"silver.{dataset}.checksum",
                    lake.sha256_hex(lake.read_bytes(s3, config.silver_bucket, key))
                    == digest,
                    object=key,
                )
            )
        primary_key = PRIMARY_KEYS[dataset]
        total, distinct = connection.execute(
            f"SELECT count(*), count(DISTINCT {primary_key}) FROM read_parquet(?::VARCHAR[], union_by_name=true)",
            [uris],
        ).fetchone()
        checks.append(
            check(
                f"silver.{dataset}.primary_key_unique",
                total == distinct,
                rows=total,
                distinct=distinct,
            )
        )
        required = REQUIRED_COLUMNS[dataset]
        nulls = connection.execute(
            "SELECT "
            + ", ".join(
                f"count(*) FILTER (WHERE {column} IS NULL)" for column in required
            )
            + " FROM read_parquet(?::VARCHAR[], union_by_name=true)",
            [uris],
        ).fetchone()
        checks.append(
            check(
                f"silver.{dataset}.required_fields",
                all(value == 0 for value in nulls),
                null_counts=dict(zip(required, nulls, strict=True)),
            )
        )
        rejected_count = connection.execute(
            "SELECT count(*) FROM read_parquet(?::VARCHAR[], union_by_name=true)",
            [rejected],
        ).fetchone()[0]
        checks.append(
            check(
                f"silver.{dataset}.quarantine_count",
                rejected_count == info["rejected_count"],
                expected=info["rejected_count"],
                observed=rejected_count,
            )
        )
        checks.append(
            check(
                f"silver.{dataset}.raw_reconciliation",
                info["row_count"] + info["rejected_count"] == info["distinct_raw_rows"],
                accepted=info["row_count"],
                rejected=info["rejected_count"],
                distinct_raw_rows=info.get("distinct_raw_rows"),
            )
        )
    checks.append(
        check(
            "silver.foreign_keys",
            connection.execute(
                "SELECT count(*) FROM read_parquet(?::VARCHAR[], union_by_name=true) p "
                "ANTI JOIN read_parquet(?::VARCHAR[], union_by_name=true) s USING (subscription_id)",
                [
                    _dataset_uris(
                        config.silver_bucket, pointer["datasets"]["payments"]["objects"]
                    ),
                    _dataset_uris(
                        config.silver_bucket,
                        pointer["datasets"]["subscriptions"]["objects"],
                    ),
                ],
            ).fetchone()[0]
            == 0,
        )
    )
    return checks


def validate_gold(s3, config, pointer=None, silver_pointer=None, windowed=False):
    pointer = pointer or lake.committed_pointer(s3, config.gold_bucket, "gold")
    silver_pointer = silver_pointer or lake.committed_pointer(
        s3, config.silver_bucket, "silver"
    )
    checks = [check("gold.committed_pointer", pointer is not None)]
    if pointer is None or silver_pointer is None:
        return checks
    connection = connect(config)
    for table in ("dim_customer", "dim_subscription", "fact_payments", "fact_service_events"):
        info = pointer["tables"][table]
        uris = _dataset_uris(config.gold_bucket, info["objects"])
        observed = connection.execute(
            "SELECT count(*) FROM read_parquet(?::VARCHAR[], union_by_name=true)",
            [uris],
        ).fetchone()[0]
        checks.append(
            check(
                f"gold.{table}.row_count",
                observed == info["row_count"],
                expected=info["row_count"],
                observed=observed,
            )
        )
        checks.append(
            check(
                f"gold.{table}.checksums",
                all(
                    lake.sha256_hex(lake.read_bytes(s3, config.gold_bucket, key))
                    == digest
                    for key, digest in info["checksums"].items()
                ),
                objects=len(info["objects"]),
            )
        )
    expected = {
        "dim_customer": silver_pointer["datasets"]["customers"]["row_count"],
        "dim_subscription": silver_pointer["datasets"]["subscriptions"]["row_count"],
        "fact_payments": silver_pointer["datasets"]["payments"]["row_count"],
        "fact_service_events": silver_pointer["datasets"]["service_events"]["row_count"],
    }
    for table, rows in expected.items():
        if windowed and table.startswith("fact_"):
            continue
        checks.append(
            check(
                f"gold.{table}.silver_reconciliation",
                pointer["tables"][table]["row_count"] == rows,
                gold_rows=pointer["tables"][table]["row_count"],
                silver_rows=rows,
            )
        )
    for table, key in (("dim_customer", "customer_key"), ("dim_subscription", "subscription_key")):
        uris = _dataset_uris(config.gold_bucket, pointer["tables"][table]["objects"])
        total, distinct = connection.execute(
            f"SELECT count(*), count(DISTINCT {key}) FROM read_parquet(?::VARCHAR[], union_by_name=true)",
            [uris],
        ).fetchone()
        checks.append(
            check(
                f"gold.{table}.surrogate_key_unique",
                total == distinct,
                rows=total,
                distinct=distinct,
            )
        )
    fact_uris = _dataset_uris(
        config.gold_bucket, pointer["tables"]["fact_payments"]["objects"]
    )
    checks.append(
        check(
            "gold.fact_payments.referential_integrity",
            connection.execute(
                "SELECT count(*) FROM read_parquet(?::VARCHAR[], union_by_name=true) "
                "WHERE customer_key IS NULL OR subscription_key IS NULL",
                [fact_uris],
            ).fetchone()[0]
            == 0,
        )
    )
    event_uris = _dataset_uris(
        config.gold_bucket, pointer["tables"]["fact_service_events"]["objects"]
    )
    checks.append(
        check(
            "gold.fact_service_events.referential_integrity",
            connection.execute(
                "SELECT count(*) FROM read_parquet(?::VARCHAR[], union_by_name=true) "
                "WHERE customer_key IS NULL",
                [event_uris],
            ).fetchone()[0]
            == 0,
        )
    )
    return checks
