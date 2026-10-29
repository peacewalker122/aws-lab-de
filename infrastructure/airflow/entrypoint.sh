#!/usr/bin/env bash
set -euo pipefail
umask 077

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://aster:${POSTGRES_PASSWORD}@postgres:5432/airflow"
export ASTER_POSTGRES_SCHEMA="${ASTER_POSTGRES_SCHEMA:-public}"

python - <<'PY'
import json
import os

path = "/opt/airflow/simple_auth_passwords.json"
descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(descriptor, "w") as handle:
    json.dump({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, handle)
os.chmod(path, 0o600)
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
