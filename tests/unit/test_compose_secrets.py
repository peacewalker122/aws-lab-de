from pathlib import Path

COMPOSE = Path(__file__).resolve().parents[2] / "infrastructure/compose.yaml"


def service_block(name):
    lines = COMPOSE.read_text().splitlines()
    start = lines.index(f"  {name}:")
    block = []
    for line in lines[start + 1 :]:
        if line.startswith("  ") and not line.startswith("   ") and line.endswith(":"):
            break
        block.append(line)
    return "\n".join(block)


def test_grafana_receives_only_its_own_admin_password():
    block = service_block("grafana")
    assert "env_file" not in block
    assert "POSTGRES_PASSWORD" not in block
    assert "AIRFLOW_ADMIN_PASSWORD" not in block
    assert "GF_SECURITY_ADMIN_PASSWORD" in block
