#!/usr/bin/env bash
set -euo pipefail

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://aster:${POSTGRES_PASSWORD}@postgres:5432/airflow"
export ASTER_POSTGRES_SCHEMA="${ASTER_POSTGRES_SCHEMA:-public}"

python - <<'PY'
import json
import os

with open("/opt/airflow/simple_auth_passwords.json", "w") as handle:
    json.dump({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, handle)
PY

case "${1:-}" in
  init)
    python -m aster --run-id "airflow-init-$(date +%s)" postgres-init --database airflow
    exec airflow db migrate
    ;;
  *)
    exec airflow "$@"
    ;;
esac
