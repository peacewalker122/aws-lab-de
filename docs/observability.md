# Local monitoring stack

The M5 stack is a real local Grafana LGTM deployment plus Prometheus and a Pushgateway. The pipeline sends telemetry to these services; nothing is mocked or synthesised by the tests.

| Component | Pinned image | Loopback port | Persistent volume |
|---|---|---|---|
| Grafana | `grafana/grafana:13.2.5` | 3000 | `grafana-data` |
| Loki | `grafana/loki:3.6.19` | 3100 | `loki-data` |
| Tempo | `grafana/tempo:2.10.10` | 3200, 4318 (OTLP HTTP) | `tempo-data` |
| Prometheus | `prom/prometheus:v3.15.1` | 19090 | `prometheus-data` |
| Pushgateway | `prom/pushgateway:v1.11.5` | 9091 | `pushgateway-data` |

Prometheus binds to 19090 on the host because 9090 is commonly occupied by an unrelated local process; inside the Compose network it still listens on 9090. Loki and Tempo images are distroless and have no shell, so they do not publish Docker health checks. Readiness is verified externally against `/ready`; `make monitoring-check` fails unless all five endpoints return HTTP 200.

## Operation

```sh
make monitoring-up       # start the stack
make monitoring-check    # assert readiness from the host
make pipeline            # run the pipeline; telemetry is pushed automatically
make test-observability  # assert logs, metrics and traces really arrived
make monitoring-down     # stop the stack, keep volumes
```

`.env` carries the loopback endpoints (`ASTER_LOKI_URL`, `ASTER_OTLP_ENDPOINT`, `ASTER_PUSHGATEWAY_URL`) and the Grafana admin password (`GF_SECURITY_ADMIN_PASSWORD`). `scripts/local_secrets.py` creates missing values with random secrets and never rewrites existing ones. Credentials stay in the untracked `.env`; no secret is committed.

Grafana: `http://127.0.0.1:3000`, user `admin`, password from `.env`. Provisioned datasources are Prometheus (default), Loki and Tempo; the dashboard `Aster Pipeline` (uid `aster-pipeline`) shows the latest run's row counts, rejected rows, run success, duration and pipeline logs.

## What the pipeline sends

- Loki: every structured JSON log line is pushed to `/loki/api/v1/push` with labels `app=aster`, `service` and `run_id`.
- Tempo: OpenTelemetry spans per stage (`extract`, `validate_raw`, `transform_silver`, `validate_silver`, `build_gold`, `validate_gold`, `publish_run`) over OTLP HTTP to `/v1/traces`.
- Prometheus: after publication, the pipeline deletes the previous `job=aster_pipeline` group in the Pushgateway and pushes `aster_rows_extracted`, `aster_rows_silver`, `aster_rows_rejected`, `aster_rows_gold`, `aster_run_duration_ms` and `aster_run_success` labelled with the run id. The Pushgateway therefore holds only the most recent run, which matches the dashboard's "latest run" panels.

Observability is best effort: an unreachable backend logs a warning and does not fail a data run. The tests assert delivery when the stack is running, so a silent failure cannot pass the suite.

## Retention

Loki keeps 168 hours and compacts TSDB chunks; Tempo retains 24-hour trace blocks; Prometheus keeps its default 15-day local TSDB. Volumes survive restarts. `make clean` removes them together with PostgreSQL and MiniStack data and requires explicit confirmation.

## Airflow

Airflow tasks receive `ASTER_LOKI_URL=http://loki:3100`, `ASTER_OTLP_ENDPOINT=http://tempo:4318` and `ASTER_PUSHGATEWAY_URL=http://pushgateway:9091`, so DAG runs emit the same telemetry through the Compose network. Start the monitoring stack before running the DAG if you want telemetry from orchestrated runs.
