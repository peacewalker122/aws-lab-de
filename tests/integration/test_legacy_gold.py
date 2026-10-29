from datetime import date

import pytest

from aster import lake, metrics, pipeline, quality
from aster.source import migrate, seed

pytestmark = pytest.mark.integration

LEGACY_KEYS = ("trend_seed", "silver", "silver_run_id")


def seeded(config):
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)


def committed_metrics(config, s3, pointer=None):
    connection = metrics.duckdb_connect(config)
    try:
        metrics.create_gold_views(connection, s3, config, pointer)
        return {
            name: metrics.run_metric(connection, name) for name in metrics.METRICS
        }
    finally:
        connection.close()


def legacy_pointer(config, s3):
    pointer = lake.committed_pointer(s3, config.gold_bucket, "gold")
    legacy = {key: value for key, value in pointer.items() if key not in LEGACY_KEYS}
    lake.put_json(s3, config.gold_bucket, "gold/_committed.json", legacy)
    return legacy


def test_legacy_gold_stays_queryable_and_rebuilds_in_place(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "legacy-a")
    before = committed_metrics(config, s3)
    objects_before = sorted(lake.list_keys(s3, config.gold_bucket, "gold/"))
    legacy = legacy_pointer(config, s3)
    assert "trend_seed" not in legacy
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == legacy

    assert committed_metrics(config, s3) == before
    assert sorted(lake.list_keys(s3, config.gold_bucket, "gold/")) == objects_before

    checks = {
        item.name: item for item in quality.validate_gold(s3, config, pointer=legacy)
    }
    assert set(checks) == {"gold.committed_pointer", "gold.pointer_contract"}
    contract = checks["gold.pointer_contract"]
    assert not contract.ok
    assert contract.detail["legacy"] is True
    assert contract.detail["missing"] == ["silver_run_id", "silver", "trend_seed"]
    assert "pipeline" in contract.detail["remedy"]

    pipeline.run_pipeline(config, "legacy-b")
    rebuilt = lake.committed_pointer(s3, config.gold_bucket, "gold")
    assert rebuilt["run_id"] == "legacy-b"
    assert rebuilt["trend_seed"]["row_count"] > 0
    assert committed_metrics(config, s3) == before
    assert set(lake.list_keys(s3, config.gold_bucket, "gold/")) >= set(objects_before)
    assert all(
        item.ok
        for item in quality.validate_gold(s3, config, pointer=rebuilt)
    )


def test_legacy_windowed_backfill_requires_a_rebuild(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "legacy-full")
    first, _ = (date.fromisoformat(value) for value in _payment_window(config, s3))
    pipeline.run_backfill(config, "legacy-backfill", first, first)
    key = "gold/backfills/legacy-backfill/manifest.json"
    pointer = lake.get_json(s3, config.gold_bucket, key)
    legacy = {name: value for name, value in pointer.items() if name not in LEGACY_KEYS}
    lake.put_json(s3, config.gold_bucket, key, legacy)
    with pytest.raises(ValueError, match="predates revenue_trend_seed"):
        committed_metrics(config, s3, legacy)
    with pytest.raises(ValueError, match="Existing gold objects are preserved"):
        committed_metrics(config, s3, legacy)
    assert lake.get_json(s3, config.gold_bucket, key) == legacy
    assert lake.list_keys(s3, config.gold_bucket, "gold/fact_payments/run=legacy-backfill/")


def _payment_window(config, s3):
    connection = metrics.duckdb_connect(config)
    try:
        metrics.create_gold_views(connection, s3, config)
        return connection.execute(
            "SELECT CAST(min(payment_date) AS VARCHAR), CAST(max(payment_date) AS VARCHAR) "
            "FROM fact_payments WHERE payment_status = 'succeeded' "
            "AND payment_date IS NOT NULL"
        ).fetchone()
    finally:
        connection.close()
