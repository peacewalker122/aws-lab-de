import json
import os
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
PLACEHOLDER = "replace-with-a-local-random-password"
DEFAULTS = {
    "AWS_ENDPOINT_URL": "http://127.0.0.1:4566",
    "AWS_DEFAULT_REGION": "us-east-1",
    "ASTER_BUCKET": "aster-local-m0-spike",
    "ASTER_LOKI_URL": "http://127.0.0.1:3100",
    "ASTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
    "ASTER_PUSHGATEWAY_URL": "http://127.0.0.1:9091",
}
SECRETS = ("POSTGRES_PASSWORD", "AIRFLOW_ADMIN_PASSWORD", "GF_SECURITY_ADMIN_PASSWORD")


def read_env():
    entries = {}
    order = []
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                entries[key.strip()] = value
                order.append(key.strip())
    return entries, order


def main():
    entries, order = read_env()
    created = []
    for key, value in DEFAULTS.items():
        if key not in entries:
            entries[key] = value
            order.append(key)
            created.append(key)
    for key in SECRETS:
        if key not in entries:
            entries[key] = secrets.token_hex(24)
            order.append(key)
            created.append(key)
    if entries["POSTGRES_PASSWORD"] == PLACEHOLDER:
        print(
            "POSTGRES_PASSWORD still holds the example placeholder; set a real local "
            "password before the first `make up`",
            file=sys.stderr,
        )
        return 1
    ENV_PATH.write_text("\n".join(f"{key}={entries[key]}" for key in order) + "\n")
    os.chmod(ENV_PATH, 0o600)
    print(
        json.dumps(
            {
                "env": str(ENV_PATH),
                "created": created,
                "note": "Airflow and Grafana receive these values through their service environment",
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
