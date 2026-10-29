import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "infrastructure/compose.yaml"
ENV_FILE = ROOT / ".env"
ENTRYPOINT = ROOT / "infrastructure/airflow/entrypoint.sh"
GRAFANA_SECRET = "GF_SECURITY_ADMIN_PASSWORD"
AIRFLOW_SERVICES = (
    "airflow",
    "airflow-init",
    "airflow-api-server",
    "airflow-scheduler",
    "airflow-dag-processor",
)


def service_block(text, name):
    lines = text.splitlines()
    start = lines.index(f"  {name}:")
    block = []
    for line in lines[start + 1 :]:
        if line.startswith("  ") and not line.startswith("   ") and line.endswith(":"):
            break
        block.append(line)
    return "\n".join(block)


def anchor_block(text, name):
    lines = text.splitlines()
    start = next(
        index for index, line in enumerate(lines) if line.startswith(f"{name}:")
    )
    block = []
    for line in lines[start + 1 :]:
        if line and not line.startswith(" "):
            break
        block.append(line)
    return "\n".join(block)


def rendered_config():
    if not shutil.which("docker") or not ENV_FILE.exists():
        pytest.skip("docker compose and a local .env are required")
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE),
            "--env-file",
            str(ENV_FILE),
            "--profile",
            "airflow",
            "--profile",
            "observability",
            "config",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        pytest.skip("docker compose config is unavailable")
    return completed.stdout


def test_grafana_receives_only_its_own_admin_password():
    block = service_block(COMPOSE.read_text(), "grafana")
    assert "env_file" not in block
    assert "POSTGRES_PASSWORD" not in block
    assert "AIRFLOW_ADMIN_PASSWORD" not in block
    assert GRAFANA_SECRET in block


def test_airflow_services_load_only_the_variables_they_need():
    text = COMPOSE.read_text()
    secrets = anchor_block(text, "x-airflow-secrets")
    assert "POSTGRES_PASSWORD" in secrets
    assert "AIRFLOW_ADMIN_PASSWORD" in secrets
    assert GRAFANA_SECRET not in secrets
    for name in AIRFLOW_SERVICES:
        block = service_block(text, name)
        assert "env_file" not in block, name
        assert "*airflow-secrets" in block, name
        assert GRAFANA_SECRET not in block, name


def test_postgres_does_not_receive_other_services_secrets():
    block = service_block(COMPOSE.read_text(), "postgres")
    assert "env_file" not in block
    assert "POSTGRES_PASSWORD" in block
    assert GRAFANA_SECRET not in block
    assert "AIRFLOW_ADMIN_PASSWORD" not in block


def test_rendered_config_keeps_the_grafana_secret_out_of_airflow():
    text = rendered_config()
    leaking = sorted(
        name
        for name in (*AIRFLOW_SERVICES, "postgres")
        if GRAFANA_SECRET in service_block(text, name)
    )
    assert leaking == []
    missing = sorted(
        name
        for name in AIRFLOW_SERVICES
        if "AIRFLOW_ADMIN_PASSWORD" not in service_block(text, name)
    )
    assert missing == []
    grafana = service_block(text, "grafana")
    assert GRAFANA_SECRET in grafana
    assert "POSTGRES_PASSWORD" not in grafana


def test_airflow_entrypoint_never_prints_its_secrets():
    script = ENTRYPOINT.read_text()
    assert "set -x" not in script
    assert script.count("POSTGRES_PASSWORD") == 1
    assert script.count("AIRFLOW_ADMIN_PASSWORD") == 1
    assert "print(" not in script
    assert "echo $" not in script


def test_running_airflow_container_has_no_grafana_secret():
    if not shutil.which("docker"):
        pytest.skip("docker is required")
    inspected = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{json .Config.Env}}",
            "infrastructure_airflow-scheduler_1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if inspected.returncode != 0:
        pytest.skip("the Airflow scheduler container is not running")
    environment = json.loads(inspected.stdout)
    names = sorted(item.split("=", 1)[0] for item in environment)
    assert GRAFANA_SECRET not in names
    assert "POSTGRES_PASSWORD" in names
    assert "AIRFLOW_ADMIN_PASSWORD" in names
