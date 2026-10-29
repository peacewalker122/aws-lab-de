from datetime import date

import pytest

from aster import lake, metrics, pipeline
from aster.source import migrate, seed
from aster.transform import DIMENSIONS, FACTS

pytestmark = pytest.mark.integration

ZERO_TABLE_COUNTS = {
    "dim_customer": 0,
    "dim_subscription": 0,
    "fact_payments": 0,
    "fact_service_events": 0,
}
ZERO_SILVER_COUNTS = {
    "customers": 0,
    "subscriptions": 0,
    "payments": 0,
    "service_events": 0,
}


def parquet_rows(config, bucket, keys):
    connection = metrics.duckdb_connect(config)
    try:
        uris = [lake.s3_uri(bucket, key) for key in keys]
        return connection.execute(
            "SELECT count(*) FROM read_parquet(?::VARCHAR[], union_by_name=true)",
            [uris],
        ).fetchone()[0]
    finally:
        connection.close()


def metric_rows(config, s3, pointer=None):
    connection = metrics.duckdb_connect(config)
    try:
        metrics.create_gold_views(connection, s3, config, pointer)
        return {
            name: metrics.run_metric(connection, name) for name in metrics.METRICS
        }
    finally:
        connection.close()


def assert_empty_facts_only(config, s3, pointer):
    assert {
        table: pointer["tables"][table]["row_count"]
        for table in DIMENSIONS + FACTS
    } == {**ZERO_TABLE_COUNTS, "dim_customer": 100, "dim_subscription": 100}
    for table in DIMENSIONS + FACTS:
        info = pointer["tables"][table]
        assert info["objects"], table
        assert parquet_rows(config, config.gold_bucket, info["objects"]) == info[
            "row_count"
        ]
    assert all(rows == [] for rows in metric_rows(config, s3, pointer).values())


def test_empty_source_publishes_valid_empty_parquet(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
    stages = pipeline.run_pipeline(config, "empty-source")
    silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    assert {
        name: info["row_count"] for name, info in silver["datasets"].items()
    } == ZERO_SILVER_COUNTS
    for info in silver["datasets"].values():
        assert info["objects"]
        assert parquet_rows(config, config.silver_bucket, info["objects"]) == 0
        assert parquet_rows(config, config.silver_bucket, info["rejected_objects"]) == 0
    gold = lake.committed_pointer(s3, config.gold_bucket, "gold")
    assert {
        table: gold["tables"][table]["row_count"] for table in DIMENSIONS + FACTS
    } == ZERO_TABLE_COUNTS
    for table in DIMENSIONS + FACTS:
        info = gold["tables"][table]
        assert info["objects"], table
        assert parquet_rows(config, config.gold_bucket, info["objects"]) == 0
    assert all(rows == [] for rows in metric_rows(config, s3, gold).values())
    assert stages["publish_run"]["metrics"] == len(metrics.METRICS)
    artifact = lake.get_json(s3, config.artifacts_bucket, "runs/empty-source/run.json")
    assert artifact["status"] == "success"
    assert artifact["row_totals"]["gold_rows"] == 0


def test_empty_window_backfill_publishes_valid_empty_parquet(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)
    pipeline.run_pipeline(config, "empty-window-full")
    full_pointer = lake.committed_pointer(s3, config.gold_bucket, "gold")
    pipeline.run_backfill(config, "empty-window", date(2020, 1, 1), date(2020, 1, 3))
    pointer = lake.get_json(
        s3, config.gold_bucket, "gold/backfills/empty-window/manifest.json"
    )
    assert pointer["window"] == ["2020-01-01", "2020-01-03"]
    assert_empty_facts_only(config, s3, pointer)
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == full_pointer
