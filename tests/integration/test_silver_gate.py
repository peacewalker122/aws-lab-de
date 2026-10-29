import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from aster import lake, pipeline
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


def stage_silver(s3, config, run_id):
    return pipeline.execute_stage(
        "transform_silver", run_id, s3, config, pipeline.transform_silver, s3, config, run_id
    )


def gate_silver(s3, config, run_id):
    return pipeline.execute_stage(
        "validate_silver",
        run_id,
        s3,
        config,
        pipeline.validate_silver_stage,
        s3,
        config,
        run_id,
    )


def publish_silver(s3, config, run_id):
    return pipeline.execute_stage(
        "publish_silver", run_id, s3, config, pipeline.publish_silver, s3, config, run_id
    )


def staged_silver(config, s3, run_id):
    return lake.get_json(
        s3, config.silver_bucket, f"silver/_staged/{run_id}.json"
    )


def committed_silver(config, s3):
    return lake.committed_pointer(s3, config.silver_bucket, "silver")


def test_build_silver_stages_without_moving_the_committed_pointer(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "stage-a")
    committed = committed_silver(config, s3)
    staged = run_aster(config, "--run-id", "stage-b", "transform-silver")
    assert json.loads(staged.stdout)["state"] == "staged"
    pointer = staged_silver(config, s3, "stage-b")
    assert pointer["state"] == "staged"
    assert pointer["run_id"] == "stage-b"
    assert committed_silver(config, s3) == committed
    assert lake.get_json(s3, config.artifacts_bucket, "runs/stage-b/run.json") is None


def test_corrupted_staged_silver_blocks_validation_and_publication(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "corrupt-a")
    committed = committed_silver(config, s3)
    stage_silver(s3, config, "corrupt-b")
    object_key = staged_silver(config, s3, "corrupt-b")["datasets"]["payments"][
        "objects"
    ][0]
    corrupt_parquet(s3, config.silver_bucket, object_key)

    rejected = run_aster(config, "--run-id", "corrupt-b", "validate-silver", check=False)
    assert rejected.returncode == 1
    assert json.loads(rejected.stdout)["error"]["type"] == "QualityError"
    assert "silver.payments.checksum" in rejected.stdout
    refused = run_aster(config, "--run-id", "corrupt-b", "publish-silver", check=False)
    assert refused.returncode == 1
    assert "no passing silver validation" in refused.stdout
    assert committed_silver(config, s3) == committed

    rebuilt = run_aster(config, "--run-id", "corrupt-c", "build-gold")
    assert rebuilt.returncode == 0
    assert json.loads(rebuilt.stdout)["silver_run_id"] == "corrupt-a"


def test_silver_publication_reverifies_bytes_after_the_gate(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "verify-a")
    committed = committed_silver(config, s3)
    stage_silver(s3, config, "verify-b")
    gate_silver(s3, config, "verify-b")
    object_key = staged_silver(config, s3, "verify-b")["datasets"]["customers"][
        "objects"
    ][0]
    original = lake.read_bytes(s3, config.silver_bucket, object_key)
    corrupt_parquet(s3, config.silver_bucket, object_key, drop_last_row=True)
    refused = run_aster(config, "--run-id", "verify-b", "publish-silver", check=False)
    assert refused.returncode == 1
    assert json.loads(refused.stdout)["error"]["type"] == "QualityError"
    assert "published.silver.customers.row_count" in refused.stdout
    assert "published.silver.customers.checksums" in refused.stdout
    assert committed_silver(config, s3) == committed

    s3.put_object(Bucket=config.silver_bucket, Key=object_key, Body=original)
    retried = run_aster(config, "--run-id", "verify-b", "publish-silver")
    assert json.loads(retried.stdout)["state"] == "published"
    assert committed_silver(config, s3)["run_id"] == "verify-b"


def test_gold_build_refuses_ungated_silver(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "gate-a")
    committed = committed_silver(config, s3)
    ungated = {key: value for key, value in committed.items() if key != "validation"}
    lake.put_json(s3, config.silver_bucket, "silver/_committed.json", ungated)

    refused = run_aster(config, "--run-id", "gate-b", "build-gold", check=False)
    assert refused.returncode == 1
    assert "not a validated snapshot" in refused.stdout
    assert "publish-silver" in refused.stdout
    assert lake.get_json(s3, config.gold_bucket, "gold/_staged/gate-b.json") is None

    for command in ("transform-silver", "validate-silver", "publish-silver"):
        run_aster(config, "--run-id", "gate-c", command)
    rebuilt = run_aster(config, "--run-id", "gate-d", "build-gold")
    assert rebuilt.returncode == 0
    assert json.loads(rebuilt.stdout)["silver_run_id"] == "gate-c"
    assert committed_silver(config, s3)["validation"]["run_id"] == "gate-c"


def test_gold_build_refuses_silver_modified_after_validation(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "modify-a")
    object_key = committed_silver(config, s3)["datasets"]["service_events"]["objects"][0]
    corrupt_parquet(s3, config.silver_bucket, object_key)

    refused = run_aster(config, "--run-id", "modify-b", "build-gold", check=False)
    assert refused.returncode == 1
    assert json.loads(refused.stdout)["error"]["type"] == "QualityError"
    assert "silver.snapshot.service_events.checksums" in refused.stdout
    assert lake.get_json(s3, config.gold_bucket, "gold/_staged/modify-b.json") is None


def test_split_cli_stages_publish_silver_only_after_validation(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    run_aster(config, "--run-id", "split-a", "extract")
    run_aster(config, "--run-id", "split-a", "validate-raw")
    run_aster(config, "--run-id", "split-a", "transform-silver")
    assert committed_silver(config, s3) is None
    premature = run_aster(config, "--run-id", "split-a", "publish-silver", check=False)
    assert premature.returncode == 1
    assert "no passing silver validation" in premature.stdout
    assert committed_silver(config, s3) is None

    validated = run_aster(config, "--run-id", "split-a", "validate-silver")
    assert json.loads(validated.stdout)["state"] == "validated"
    published = run_aster(config, "--run-id", "split-a", "publish-silver")
    assert json.loads(published.stdout)["state"] == "published"
    retried = run_aster(config, "--run-id", "split-a", "publish-silver")
    assert json.loads(retried.stdout)["state"] == "published"
    assert committed_silver(config, s3)["run_id"] == "split-a"

    run_aster(config, "--run-id", "split-a", "build-gold")
    run_aster(config, "--run-id", "split-a", "validate-gold")
    run_aster(config, "--run-id", "split-a", "publish-run")
    assert lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"] == "split-a"


def test_interleaved_silver_stage_cannot_roll_back_the_committed_pointer(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    run_aster(config, "--run-id", "inter-base", "extract")
    stage_silver(s3, config, "inter-a")
    stage_silver(s3, config, "inter-b")
    gate_silver(s3, config, "inter-b")
    publish_silver(s3, config, "inter-b")
    gate_silver(s3, config, "inter-a")
    with pytest.raises(ValueError, match="refusing to publish another run's snapshot"):
        publish_silver(s3, config, "inter-a")
    assert committed_silver(config, s3)["run_id"] == "inter-b"
