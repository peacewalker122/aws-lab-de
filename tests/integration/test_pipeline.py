import pytest

from aster import lake, metrics, pipeline
from aster.ingestion import ingest
from aster.source import migrate, seed

pytestmark = pytest.mark.integration


def seeded(config):
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)


def committed_metrics(config, s3, pointer=None):
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


def test_pipeline_publishes_all_zones_with_correct_rows(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "e2e-1")
    silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    gold = lake.committed_pointer(s3, config.gold_bucket, "gold")
    assert silver["run_id"] == "e2e-1"
    assert gold["run_id"] == "e2e-1"
    assert {name: info["row_count"] for name, info in silver["datasets"].items()} == {
        "customers": 100,
        "subscriptions": 100,
        "payments": 100,
        "service_events": 1000,
    }
    assert all(
        info["rejected_count"] == 0 for info in silver["datasets"].values()
    )
    assert {name: info["row_count"] for name, info in gold["tables"].items()} == {
        "dim_customer": 100,
        "dim_subscription": 100,
        "fact_payments": 100,
        "fact_service_events": 1000,
    }
    manifests = {
        dataset: len(lake.raw_manifests(s3, config.raw_bucket, dataset))
        for dataset in ("customers", "subscriptions", "payments", "service_events")
    }
    assert manifests == {
        "customers": 1,
        "subscriptions": 1,
        "payments": 1,
        "service_events": 4,
    }
    artifact = lake.get_json(s3, config.artifacts_bucket, "runs/e2e-1/run.json")
    assert artifact["status"] == "success"
    assert artifact["row_totals"] == {
        "extracted": 1300,
        "silver_rows": 1300,
        "rejected_rows": 0,
        "gold_rows": 1300,
    }
    stored_metrics = lake.get_json(
        s3, config.artifacts_bucket, "runs/e2e-1/metrics.json"
    )
    assert sorted(stored_metrics) == sorted(metrics.METRICS)
    assert len(stored_metrics["daily_event_volume"]) > 300
    assert sum(
        float(row["revenue"]) for row in stored_metrics["revenue_by_region"]
    ) > 0
    assert len(stored_metrics["p95_service_duration_by_day"]) > 300


def test_replay_of_committed_batches_is_logically_idempotent(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "replay-1")
    before = committed_metrics(config, s3)
    before_keys = {
        zone: sorted(lake.list_keys(s3, lake_env.buckets[zone], ""))
        for zone in ("raw", "silver", "gold")
    }
    with pipeline.connect_postgres(config) as connection:
        connection.execute(
            "UPDATE extraction_checkpoints SET cursor_time = '0001-01-01 00:00:00+00', "
            "cursor_id = '00000000-0000-0000-0000-000000000000' "
            "WHERE dataset IN ('customers', 'payments')"
        )
    with pipeline.connect_postgres(config) as connection:
        replayed = ingest(connection, s3, config, "replay-2")
    assert replayed["customers"] == 100
    assert replayed["payments"] == 100
    after_raw_keys = sorted(lake.list_keys(s3, lake_env.buckets["raw"], ""))
    assert after_raw_keys == before_keys["raw"]
    pipeline.run_pipeline(config, "replay-2")
    assert committed_metrics(config, s3) == before
    silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    assert silver["datasets"]["customers"]["row_count"] == 100
