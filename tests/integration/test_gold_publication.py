import io
import json
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from aster import lake, metrics, pipeline, quality
from aster.source import migrate, seed
from conftest import run_aster

pytestmark = pytest.mark.integration


def seeded(config):
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)


def corrupt_parquet(s3, bucket, key):
    original = lake.read_bytes(s3, bucket, key)
    table = pq.read_table(io.BytesIO(original))
    corrupted = table.append_column(
        "corruption_marker", pa.array([1] * table.num_rows, type=pa.int32())
    )
    buffer = io.BytesIO()
    pq.write_table(corrupted, buffer, compression="zstd")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    assert lake.read_bytes(s3, bucket, key) != original


def metrics_for_pointer(config, s3, pointer):
    connection = metrics.duckdb_connect(config)
    try:
        metrics.create_gold_views(connection, s3, config, pointer)
        return json.loads(
            json.dumps(
                {name: metrics.run_metric(connection, name) for name in metrics.METRICS},
                default=str,
            )
        )
    finally:
        connection.close()


def committed_metrics(config, s3):
    return metrics_for_pointer(
        config, s3, lake.committed_pointer(s3, config.gold_bucket, "gold")
    )


def payment_window(config, s3):
    connection = metrics.duckdb_connect(config)
    try:
        metrics.create_gold_views(connection, s3, config)
        first, last = connection.execute(
            "SELECT CAST(min(payment_date) AS VARCHAR), CAST(max(payment_date) AS VARCHAR) "
            "FROM fact_payments WHERE payment_status = 'succeeded' "
            "AND payment_date IS NOT NULL"
        ).fetchone()
    finally:
        connection.close()
    return date.fromisoformat(first), date.fromisoformat(last)


def staged_pointer(config, s3, run_id):
    return lake.get_json(s3, config.gold_bucket, f"gold/_staged/{run_id}.json")


def test_build_gold_stages_without_moving_the_committed_pointer(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "stage-a")
    committed = lake.committed_pointer(s3, config.gold_bucket, "gold")
    pipeline.execute_stage(
        "build_gold", "stage-b", s3, config, pipeline.make_gold, s3, config, "stage-b"
    )
    pointer = staged_pointer(config, s3, "stage-b")
    assert pointer["state"] == "staged"
    assert pointer["run_id"] == "stage-b"
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed
    assert lake.get_json(s3, config.artifacts_bucket, "runs/stage-b/run.json") is None
    stage = lake.get_json(
        s3, config.artifacts_bucket, "runs/stage-b/stages/build_gold.json"
    )
    assert stage["state"] == "staged"
    assert stage["staged_pointer"] == "gold/_staged/stage-b.json"


def test_corrupted_staged_object_blocks_full_publication(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "gate-a")
    committed = lake.committed_pointer(s3, config.gold_bucket, "gold")
    before = committed_metrics(config, s3)
    pipeline.execute_stage(
        "build_gold", "gate-b", s3, config, pipeline.make_gold, s3, config, "gate-b"
    )
    object_key = staged_pointer(config, s3, "gate-b")["tables"]["dim_customer"][
        "objects"
    ][0]
    corrupt_parquet(s3, config.gold_bucket, object_key)
    with pytest.raises(quality.QualityError) as failed:
        pipeline.execute_stage(
            "validate_gold",
            "gate-b",
            s3,
            config,
            pipeline.validate_gold_stage,
            s3,
            config,
            "gate-b",
        )
    failing = {item.name for item in failed.value.checks if not item.ok}
    assert "gold.dim_customer.checksums" in failing
    with pytest.raises(ValueError, match="no passing gold validation"):
        pipeline.publish(s3, config, "gate-b")
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed
    assert committed_metrics(config, s3) == before
    assert lake.get_json(s3, config.artifacts_bucket, "runs/gate-b/run.json") is None


def test_cli_stages_publish_only_after_the_gate_passes(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    run_aster(config, "--run-id", "cli-a", "pipeline")
    built = run_aster(config, "--run-id", "cli-b", "build-gold")
    assert json.loads(built.stdout)["state"] == "staged"
    object_key = staged_pointer(config, s3, "cli-b")["tables"]["fact_payments"][
        "objects"
    ][0]
    corrupt_parquet(s3, config.gold_bucket, object_key)
    direct = run_aster(config, "--run-id", "cli-b", "publish-run", check=False)
    assert direct.returncode == 1
    assert "no passing gold validation" in direct.stdout
    rejected = run_aster(config, "--run-id", "cli-b", "validate-gold", check=False)
    assert rejected.returncode == 1
    assert json.loads(rejected.stdout)["error"]["type"] == "QualityError"
    blocked = run_aster(config, "--run-id", "cli-b", "publish-run", check=False)
    assert blocked.returncode == 1
    assert (
        lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"] == "cli-a"
    )
    assert lake.get_json(s3, config.artifacts_bucket, "runs/cli-b/run.json") is None


def test_backfill_publishes_only_after_validation_and_records_its_metrics(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "bf-full")
    committed = lake.committed_pointer(s3, config.gold_bucket, "gold")
    start, _ = payment_window(config, s3)
    window = (start, start)
    pipeline.execute_stage(
        "build_gold",
        "bf-a",
        s3,
        config,
        pipeline.make_gold,
        s3,
        config,
        "bf-a",
        window,
    )
    assert (
        lake.get_json(s3, config.gold_bucket, "gold/backfills/bf-a/manifest.json")
        is None
    )
    assert staged_pointer(config, s3, "bf-a")["window"] == [
        start.isoformat(),
        start.isoformat(),
    ]
    seed_key = staged_pointer(config, s3, "bf-a")["trend_seed"]["objects"][0]
    corrupt_parquet(s3, config.gold_bucket, seed_key)
    with pytest.raises(quality.QualityError):
        pipeline.execute_stage(
            "validate_gold",
            "bf-a",
            s3,
            config,
            pipeline.validate_gold_stage,
            s3,
            config,
            "bf-a",
        )
    with pytest.raises(ValueError, match="no passing gold validation"):
        pipeline.publish(s3, config, "bf-a")
    assert (
        lake.get_json(s3, config.gold_bucket, "gold/backfills/bf-a/manifest.json")
        is None
    )
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed

    pipeline.execute_stage(
        "build_gold",
        "bf-b",
        s3,
        config,
        pipeline.make_gold,
        s3,
        config,
        "bf-b",
        window,
    )
    pipeline.execute_stage(
        "validate_gold",
        "bf-b",
        s3,
        config,
        pipeline.validate_gold_stage,
        s3,
        config,
        "bf-b",
    )
    pipeline.execute_stage(
        "publish_run", "bf-b", s3, config, pipeline.publish, s3, config, "bf-b"
    )
    pointer = lake.get_json(
        s3, config.gold_bucket, "gold/backfills/bf-b/manifest.json"
    )
    assert pointer["state"] == "published"
    assert pointer["window"] == [start.isoformat(), start.isoformat()]
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed
    artifact = lake.get_json(s3, config.artifacts_bucket, "runs/bf-b/run.json")
    assert artifact["gold_state"] == "published"
    assert artifact["gold_pointer"] == "gold/backfills/bf-b/manifest.json"
    assert lake.get_json(
        s3, config.artifacts_bucket, "runs/bf-b/metrics.json"
    ) == metrics_for_pointer(config, s3, pointer)
