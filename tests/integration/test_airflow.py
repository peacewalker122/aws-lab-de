import hashlib
import io
import os
import re
import subprocess

import pytest

import pyarrow.parquet as pq

from aster import lake, pipeline
from aster.ingestion import ingest
from aster.source import migrate, seed

pytestmark = [pytest.mark.integration, pytest.mark.airflow]

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
COMPOSE = [
    "docker",
    "compose",
    "-f",
    "infrastructure/compose.yaml",
    "--env-file",
    ".env",
    "--profile",
    "airflow",
]
CONNECTION = (
    'export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://aster:$POSTGRES_PASSWORD@postgres:5432/airflow"; '
)


def isolated_environment(config):
    values = {
        "ASTER_POSTGRES_SCHEMA": config.postgres_schema,
        "ASTER_RAW_BUCKET": config.raw_bucket,
        "ASTER_SILVER_BUCKET": config.silver_bucket,
        "ASTER_GOLD_BUCKET": config.gold_bucket,
        "ASTER_ARTIFACTS_BUCKET": config.artifacts_bucket,
        "ASTER_LOKI_URL": "",
        "ASTER_OTLP_ENDPOINT": "",
        "ASTER_PUSHGATEWAY_URL": "",
    }
    return "export " + " ".join(
        f'{key}="{value}"' for key, value in values.items()
    ) + "; "


def airflow(config, *arguments, check=True):
    prefix = isolated_environment(config) if config else ""
    return subprocess.run(
        COMPOSE
        + [
            "exec",
            "-T",
            "airflow-scheduler",
            "bash",
            "-c",
            prefix + CONNECTION + " ".join(arguments),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def require_airflow():
    running = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            "name=infrastructure_airflow-scheduler_1",
            "--filter",
            "status=running",
            "--format",
            "{{.Names}}",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if not running:
        pytest.skip("Airflow services are not running; run `make airflow-up`")
    if not os.environ.get("POSTGRES_PASSWORD"):
        pytest.skip("local Aster environment is not configured")


def seeded(config):
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)


def sanitized_run_id(output):
    match = re.search(r"run_id=(manual__[^,\s\"]+)", output)
    if not match:
        raise AssertionError("Airflow output did not report a run id")
    return match.group(1).replace(".", "-").replace(":", "-").replace("+", "-")


def corrupt_first_raw_object(config, s3, dataset):
    manifest = lake.raw_manifests(s3, config.raw_bucket, dataset)[0]
    key = manifest["objects"][0]["key"]
    table = pq.read_table(io.BytesIO(lake.read_bytes(s3, config.raw_bucket, key)))
    buffer = io.BytesIO()
    pq.write_table(table.slice(0, table.num_rows - 1), buffer, compression="zstd")
    s3.put_object(Bucket=config.raw_bucket, Key=key, Body=buffer.getvalue())
    stored = lake.read_bytes(s3, config.raw_bucket, key)
    assert hashlib.sha256(stored).hexdigest() != manifest["objects"][0]["sha256"]
    return key


def test_dag_is_registered():
    require_airflow()
    listed = airflow(None, "airflow", "dags", "list").stdout
    assert "aster_pipeline" in listed
    assert "aster_pipeline.py" in listed


def test_airflow_run_completes_in_isolated_schema_and_buckets(lake_env):
    require_airflow()
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    completed = airflow(config, "airflow", "dags", "test", "aster_pipeline", "2026-01-04")
    output = completed.stdout + completed.stderr
    assert "state=success" in output
    run_id = sanitized_run_id(output)
    artifact = lake.get_json(
        s3, config.artifacts_bucket, f"runs/{run_id}/run.json"
    )
    assert artifact is not None
    assert artifact["status"] == "success"
    assert sorted(artifact["stages"]) == [
        "build_gold",
        "extract",
        "transform_silver",
        "validate_gold",
        "validate_raw",
        "validate_silver",
    ]
    assert lake.get_json(
        s3, config.artifacts_bucket, f"runs/{run_id}/stages/publish_run.json"
    )["stage"] == "publish_run"
    metrics = lake.get_json(
        s3, config.artifacts_bucket, f"runs/{run_id}/metrics.json"
    )
    assert len(metrics) == 8
    staged = lake.get_json(s3, config.gold_bucket, f"gold/_staged/{run_id}.json")
    assert staged["state"] == "staged"
    assert staged["silver_run_id"] == run_id
    gold = lake.committed_pointer(s3, config.gold_bucket, "gold")
    assert gold is not None
    assert gold["run_id"] == run_id
    assert gold["tables"]["fact_service_events"]["row_count"] == 1000
    silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    assert silver["run_id"] == run_id


def test_failed_quality_gate_blocks_publication(lake_env):
    require_airflow()
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    with pipeline.connect_postgres(config) as connection:
        ingest(connection, s3, config, "pre-ingest")
    corrupted = corrupt_first_raw_object(config, s3, "customers")
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") is None
    failed = airflow(
        config, "airflow", "dags", "test", "aster_pipeline", "2026-01-05", check=False
    )
    output = failed.stdout + failed.stderr
    assert failed.returncode != 0
    assert "state=failed" in output
    assert '"command": "validate-raw"' in output
    assert '"status": "failed"' in output
    run_id = sanitized_run_id(output)
    stages = sorted(
        key.rsplit("/", 1)[-1][: -len(".json")]
        for key in lake.list_keys(
            s3, config.artifacts_bucket, f"runs/{run_id}/stages/"
        )
    )
    assert stages == ["extract"]
    assert (
        lake.get_json(s3, config.artifacts_bucket, f"runs/{run_id}/run.json") is None
    )
    assert lake.list_keys(s3, config.gold_bucket, "") == []
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") is None
    assert lake.read_bytes(s3, config.raw_bucket, corrupted)


def test_gold_quality_gate_blocks_staged_output_in_airflow(lake_env):
    require_airflow()
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    run_id = "airflow-gold-gate"
    for command in (
        ("transform-silver",),
        ("validate-silver",),
        ("build-gold",),
    ):
        airflow(config, "python", "-m", "aster", "--run-id", run_id, *command)
    assert (
        lake.get_json(s3, config.gold_bucket, f"gold/_staged/{run_id}.json")
        is not None
    )
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") is None
    staged = lake.get_json(s3, config.gold_bucket, f"gold/_staged/{run_id}.json")
    object_key = staged["tables"]["fact_service_events"]["objects"][0]
    table = pq.read_table(
        io.BytesIO(lake.read_bytes(s3, config.gold_bucket, object_key))
    )
    buffer = io.BytesIO()
    pq.write_table(table.slice(0, table.num_rows - 1), buffer, compression="zstd")
    s3.put_object(Bucket=config.gold_bucket, Key=object_key, Body=buffer.getvalue())

    rejected = airflow(
        config, "python", "-m", "aster", "--run-id", run_id, "validate-gold",
        check=False,
    )
    assert rejected.returncode != 0
    assert '"command": "validate-gold"' in rejected.stdout
    blocked = airflow(
        config, "python", "-m", "aster", "--run-id", run_id, "publish-run",
        check=False,
    )
    assert blocked.returncode != 0
    assert "no passing gold validation" in blocked.stdout
    assert lake.committed_pointer(s3, config.gold_bucket, "gold") is None
    assert lake.get_json(s3, config.artifacts_bucket, f"runs/{run_id}/run.json") is None


def test_airflow_admin_password_file_is_owner_only(lake_env):
    require_airflow()
    mode = airflow(
        lake_env.config, "stat", "-c", "%a", "/opt/airflow/simple_auth_passwords.json"
    ).stdout.strip()
    assert mode == "600"
