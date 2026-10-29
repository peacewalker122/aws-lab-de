import io
import json
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
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


def corrupt_parquet(s3, bucket, key, drop_last_row=False):
    original = lake.read_bytes(s3, bucket, key)
    table = pq.read_table(io.BytesIO(original))
    corrupted = (
        table.slice(0, table.num_rows - 1)
        if drop_last_row
        else table.append_column(
            "corruption_marker", pa.array([1] * table.num_rows, type=pa.int32())
        )
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
                {
                    name: metrics.run_metric(connection, name)
                    for name in metrics.METRICS
                },
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


def test_gold_validates_against_the_silver_snapshot_it_was_built_from(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "snap-a")
    committed = lake.committed_pointer(s3, config.gold_bucket, "gold")
    pipeline.execute_stage(
        "build_gold", "snap-b", s3, config, pipeline.make_gold, s3, config, "snap-b"
    )
    pointer = staged_pointer(config, s3, "snap-b")
    assert pointer["silver_run_id"] == "snap-a"
    assert pointer["silver"]["run_id"] == "snap-a"
    modified = rewrite_silver_payments(config, s3, "snap-silver-2")
    assert (
        modified["datasets"]["payments"]["row_count"]
        == (pointer["silver"]["datasets"]["payments"]["row_count"])
    )
    assert payments_total(config, s3, modified) != payments_total(
        config, s3, pointer["silver"]
    )

    checks = {
        item.name: item for item in quality.validate_gold(s3, config, pointer=pointer)
    }
    assert checks["gold.silver_snapshot"].ok
    assert checks["gold.fact_payments.silver_reconciliation"].ok
    assert not checks["gold.silver_snapshot_current"].ok
    assert checks["gold.silver_snapshot_current"].detail["gold_silver_run"] == "snap-a"
    assert (
        checks["gold.silver_snapshot_current"].detail["committed_silver_run"]
        == "snap-silver-2"
    )

    with pytest.raises(quality.QualityError) as failed:
        pipeline.execute_stage(
            "validate_gold",
            "snap-b",
            s3,
            config,
            pipeline.validate_gold_stage,
            s3,
            config,
            "snap-b",
        )
    failing = {item.name for item in failed.value.checks if not item.ok}
    assert failing == {"gold.silver_snapshot_current"}
    with pytest.raises(ValueError, match="no passing gold validation"):
        pipeline.publish(s3, config, "snap-b")
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed
    assert payments_total(config, s3, committed["silver"]) != payments_total(
        config, s3, modified
    )


def rewrite_silver_payments(config, s3, run_id):
    pointer = lake.committed_pointer(s3, config.silver_bucket, "silver")
    info = pointer["datasets"]["payments"]
    source = lake.read_bytes(s3, config.silver_bucket, info["objects"][0])
    table = pq.read_table(io.BytesIO(source))
    amount = table.column("amount")
    modified = table.set_column(
        table.schema.get_field_index("amount"),
        "amount",
        pc.add(amount, pa.scalar(1, type=amount.type)),
    )
    buffer = io.BytesIO()
    pq.write_table(modified, buffer, compression="zstd")
    body = buffer.getvalue()
    key = f"silver/payments/run={run_id}/data.parquet"
    s3.put_object(Bucket=config.silver_bucket, Key=key, Body=body)
    datasets = dict(pointer["datasets"])
    datasets["payments"] = {
        **info,
        "objects": [key],
        "checksums": {key: lake.sha256_hex(body)},
    }
    rewritten = {
        **pointer,
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "datasets": datasets,
    }
    lake.put_json(s3, config.silver_bucket, "silver/_committed.json", rewritten)
    return rewritten


def payments_total(config, s3, pointer):
    connection = metrics.duckdb_connect(config)
    try:
        connection.execute(
            "CREATE OR REPLACE VIEW silver_payments AS SELECT * FROM read_parquet(["
            + ", ".join(
                f"'{lake.s3_uri(config.silver_bucket, key)}'"
                for key in pointer["datasets"]["payments"]["objects"]
            )
            + "], union_by_name=true)"
        )
        return connection.execute("SELECT sum(amount) FROM silver_payments").fetchone()[
            0
        ]
    finally:
        connection.close()


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
    assert lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"] == "cli-a"
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
    pointer = lake.get_json(s3, config.gold_bucket, "gold/backfills/bf-b/manifest.json")
    assert pointer["state"] == "published"
    assert pointer["window"] == [start.isoformat(), start.isoformat()]
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed
    artifact = lake.get_json(s3, config.artifacts_bucket, "runs/bf-b/run.json")
    assert artifact["gold_state"] == "published"
    assert artifact["gold_pointer"] == "gold/backfills/bf-b/manifest.json"
    assert lake.get_json(
        s3, config.artifacts_bucket, "runs/bf-b/metrics.json"
    ) == metrics_for_pointer(config, s3, pointer)


def test_publication_reverifies_gold_bytes_after_a_passing_gate(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "reverify-a")
    committed = lake.committed_pointer(s3, config.gold_bucket, "gold")
    before = committed_metrics(config, s3)
    pipeline.execute_stage(
        "build_gold",
        "reverify-b",
        s3,
        config,
        pipeline.make_gold,
        s3,
        config,
        "reverify-b",
    )
    pipeline.execute_stage(
        "validate_gold",
        "reverify-b",
        s3,
        config,
        pipeline.validate_gold_stage,
        s3,
        config,
        "reverify-b",
    )
    pointer = staged_pointer(config, s3, "reverify-b")
    object_key = pointer["tables"]["fact_payments"]["objects"][0]
    corrupt_parquet(s3, config.gold_bucket, object_key, drop_last_row=True)

    with pytest.raises(quality.QualityError) as refused:
        pipeline.publish(s3, config, "reverify-b")
    assert "published.gold.fact_payments.row_count" in str(refused.value)
    assert "published.gold.fact_payments.checksums" in str(refused.value)
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed
    assert committed_metrics(config, s3) == before
    assert (
        lake.get_json(s3, config.artifacts_bucket, "runs/reverify-b/run.json") is None
    )


def test_publication_refuses_embedded_silver_corrupted_after_the_gold_gate(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "embed-a")
    committed = lake.committed_pointer(s3, config.gold_bucket, "gold")
    pipeline.execute_stage(
        "build_gold", "embed-b", s3, config, pipeline.make_gold, s3, config, "embed-b"
    )
    pipeline.execute_stage(
        "validate_gold",
        "embed-b",
        s3,
        config,
        pipeline.validate_gold_stage,
        s3,
        config,
        "embed-b",
    )
    silver_key = lake.committed_pointer(s3, config.silver_bucket, "silver")["datasets"][
        "payments"
    ]["objects"][0]
    corrupt_parquet(s3, config.silver_bucket, silver_key, drop_last_row=True)

    with pytest.raises(quality.QualityError) as refused:
        pipeline.publish(s3, config, "embed-b")
    assert "published.gold.silver.payments.row_count" in str(refused.value)
    assert "published.gold.silver.payments.checksums" in str(refused.value)
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed


def test_final_stage_record_failure_reports_published_gold(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "stage-record-a")
    failed = run_aster(
        config,
        "--run-id",
        "stage-record-a",
        "publish-run",
        extra_env={"ASTER_FAULT_FAILURE": "publish_run-stage-record"},
        check=False,
    )
    assert failed.returncode == 1
    assert (
        lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"]
        == "stage-record-a"
    )
    record = lake.get_json(s3, config.artifacts_bucket, "runs/stage-record-a/run.json")
    assert record["status"] == "failed"
    assert record["gold_state"] == "published"
    assert record["error"]["type"] == "InjectedFault"


def test_artifact_failure_after_the_pointer_commit_records_published_state(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    failed = run_aster(
        config,
        "--run-id",
        "artifact-a",
        "pipeline",
        extra_env={"ASTER_FAULT_FAILURE": "publish-artifact-write"},
        check=False,
    )
    assert failed.returncode == 1
    assert json.loads(failed.stdout)["error"]["type"] == "InjectedFault"
    pointer = lake.committed_pointer(s3, config.gold_bucket, "gold")
    assert pointer["run_id"] == "artifact-a"
    assert pointer["state"] == "published"
    record = lake.get_json(s3, config.artifacts_bucket, "runs/artifact-a/run.json")
    assert record["status"] == "failed"
    assert record["gold_state"] == "published"
    assert record["gold_pointer"] == "gold/_committed.json"
    assert record["error"]["type"] == "InjectedFault"
    assert (
        len(lake.get_json(s3, config.artifacts_bucket, "runs/artifact-a/metrics.json"))
        == 8
    )
    assert metrics_for_pointer(config, s3, pointer)

    retried = run_aster(config, "--run-id", "artifact-a", "publish-run")
    assert json.loads(retried.stdout)["status"] == "ok"
    record = lake.get_json(s3, config.artifacts_bucket, "runs/artifact-a/run.json")
    assert record["status"] == "success"
    assert record["gold_state"] == "published"
    assert lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"] == (
        "artifact-a"
    )


def test_windowed_pipeline_artifact_failure_records_published_state(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "artifact-base")
    committed = lake.committed_pointer(s3, config.gold_bucket, "gold")
    start, _ = payment_window(config, s3)
    run_id = "artifact-window"
    failed = run_aster(
        config,
        "--run-id",
        run_id,
        "pipeline",
        "--start",
        start.isoformat(),
        "--end",
        start.isoformat(),
        extra_env={"ASTER_FAULT_FAILURE": "publish-artifact-write"},
        check=False,
    )
    assert failed.returncode == 1
    assert json.loads(failed.stdout)["error"]["type"] == "InjectedFault"
    pointer_key = f"gold/backfills/{run_id}/manifest.json"
    pointer = lake.get_json(s3, config.gold_bucket, pointer_key)
    assert pointer["state"] == "published"
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed
    record = lake.get_json(s3, config.artifacts_bucket, f"runs/{run_id}/run.json")
    assert record["status"] == "failed"
    assert record["gold_state"] == "published"
    assert record["gold_pointer"] == pointer_key
    assert record["error"]["type"] == "InjectedFault"
    assert (
        len(lake.get_json(s3, config.artifacts_bucket, f"runs/{run_id}/metrics.json"))
        == 8
    )


def test_gold_validation_requires_a_checksum_for_every_output_object(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "checksum-base")
    committed = lake.committed_pointer(s3, config.gold_bucket, "gold")
    pipeline.execute_stage(
        "build_gold",
        "checksum-missing",
        s3,
        config,
        pipeline.make_gold,
        s3,
        config,
        "checksum-missing",
    )
    pointer = staged_pointer(config, s3, "checksum-missing")
    info = pointer["tables"]["dim_customer"]
    info["checksums"].pop(info["objects"][0])
    lake.put_json(s3, config.gold_bucket, "gold/_staged/checksum-missing.json", pointer)

    with pytest.raises(quality.QualityError) as refused:
        pipeline.execute_stage(
            "validate_gold",
            "checksum-missing",
            s3,
            config,
            pipeline.validate_gold_stage,
            s3,
            config,
            "checksum-missing",
        )
    assert "gold.dim_customer.checksums" in str(refused.value)
    with pytest.raises(ValueError, match="no passing gold validation"):
        pipeline.publish(s3, config, "checksum-missing")
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") == committed
