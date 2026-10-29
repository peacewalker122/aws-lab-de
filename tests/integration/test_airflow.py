import os
import re
import subprocess

import pytest

from aster import lake
from aster.config import Config

pytestmark = [pytest.mark.integration, pytest.mark.airflow]

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
COMPOSE = ["docker", "compose", "-f", "infrastructure/compose.yaml", "--profile", "airflow"]
CONNECTION = (
    'export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://aster:$POSTGRES_PASSWORD@postgres:5432/airflow"; '
)


def airflow(*arguments, check=True):
    return subprocess.run(
        COMPOSE
        + [
            "exec",
            "-T",
            "airflow-scheduler",
            "bash",
            "-c",
            CONNECTION + " ".join(arguments),
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


def sanitized_run_id(output, logical_date):
    match = re.search(r"run_id=(manual__[^,\s]+)", output)
    if not match:
        raise AssertionError("Airflow output did not report a run id")
    return match.group(1).replace(".", "-").replace(":", "-").replace("+", "-")


def test_dag_is_registered():
    require_airflow()
    listed = airflow("airflow", "dags", "list").stdout
    assert "aster_pipeline" in listed
    assert "orchestration/dags/aster_pipeline.py" in listed or "aster_pipeline.py" in listed


def test_airflow_run_completes_and_publishes():
    require_airflow()
    logical_date = "2026-01-04"
    completed = airflow("airflow", "dags", "test", "aster_pipeline", logical_date)
    output = completed.stdout + completed.stderr
    assert "state=success" in output
    run_id = sanitized_run_id(output, logical_date)
    config = Config.from_env()
    s3 = lake.s3_client(config)
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
    gold = lake.committed_pointer(s3, config.gold_bucket, "gold")
    assert gold is not None and gold["tables"]["fact_service_events"]["row_count"] > 0


def test_airflow_quality_failure_propagates_and_skips_publication():
    require_airflow()
    failed = airflow(
        "airflow",
        "dags",
        "test",
        "aster_pipeline",
        "2026-01-05",
        "--conf",
        '\'{"window_start": "2025-12-31", "window_end": "2025-01-01"}\'',
        check=False,
    )
    output = failed.stdout + failed.stderr
    assert failed.returncode != 0
    assert "state=failed" in output
    assert "publish_run" in output
    assert "DAG TEST] starting task_id=publish_run" not in output
