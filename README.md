# Aster Data Platform

Aster is a local AWS-compatible proof of concept for a fictional telecom/SaaS business. Milestones M1 through M5 are implemented and tested: local AWS-compatible infrastructure, incremental ingestion, silver/gold Parquet modeling, eight DuckDB metrics, Airflow orchestration, five reliability scenarios, and a real local Grafana LGTM monitoring stack. M6 portfolio packaging is not implemented. Everything is local emulation; no AWS deployment or AWS performance is claimed. See [project.md](project.md) for requirements and milestones.

## Prerequisites

- Docker with Compose and a running container engine
- Terraform 1.9 or newer (below 2.0)
- uv and Python 3.12 or 3.13
- curl and make
- Network access to download pinned images, the Terraform provider and Python packages

Terraform accepts only loopback endpoints and uses dummy AWS credentials. Never use production credentials. PostgreSQL and MiniStack ports bind to loopback only.

## Start the lab

Create an untracked local configuration once. This command generates a local PostgreSQL password without displaying it:

```sh
cp .env.example .env
python3 -c 'import secrets; p=".env"; s=open(p).read().replace("replace-with-a-local-random-password", secrets.token_hex(24)); open(p,"w").write(s)'
chmod 600 .env
uv sync --locked
make up
make doctor
make infra-init
make infra-apply
make infra-plan
make test
make test-integration
```

Wait for both health checks before provisioning or testing; retry `make doctor` if startup is still in progress. If Terraform is installed through mise without activation, prefix Make commands with `mise exec terraform@1.11.4 --`. The integration tests connect to PostgreSQL, check the lake buckets, and exercise ingestion in an isolated schema and bucket.

Terraform provisions `aster-local-raw`, `aster-local-silver`, `aster-local-gold`, and `aster-local-artifacts`, plus the retained `aster-local-m0-spike` bucket used by the M0 checks. `make athena` runs an optional constant query against MiniStack, not a lake query. See [compatibility evidence](docs/compatibility.md).

To change the local Terraform endpoint, set `TF_VAR_endpoint`; set `AWS_ENDPOINT_URL` in `.env` to the same URL for tests. Both must use loopback. Set `TF_VAR_environment` in the shell and in `.env` for tests if changing bucket names; set `ASTER_BUCKET` to the corresponding `aster-<environment>-m0-spike`. For Terraform region changes, set `TF_VAR_region` and match `AWS_DEFAULT_REGION` in `.env`. The `.env` file is loaded by Compose for PostgreSQL and sourced by `make doctor`, `make test-integration`, and `make athena`; only use a trusted file. Terraform credentials are dummy values.

## Incremental ingestion

After provisioning, `make migrate` initializes source tables and cursor triggers. `make seed` inserts a synthetic small dataset only when all source tables are empty; set `ASTER_SEED_SIZE` to `small`, `medium`, or `large` and `ASTER_SEED` to an integer to vary it. `make ingest` extracts batches into the raw bucket; `ASTER_BATCH_SIZE` controls batch size. Set overrides in `.env`.

Source triggers assign monotonically increasing cursor timestamps using a transactional clock row. Extraction takes a shared table lock for each batch, then advances its PostgreSQL checkpoint only after verifying S3 data and manifest. This serializes writes during each batch and trades throughput for correctness. Source deletes and event updates are rejected; disabling triggers or writing outside this contract invalidates cursor guarantees. Raw batches published before a checkpoint rollback are replayed idempotently with identical object keys and manifests. Integration tests cover concurrent writers, cursor ties, and interrupted checkpoint commits.

## Analytical pipeline

`make pipeline` runs extraction, raw validation, silver transformation, silver validation, gold modeling, gold validation, and publication in one run. `RUN_ID` defaults to a UTC timestamp; override it with `make pipeline RUN_ID=my-run`. Individual stages are available as `make validate`, `make transform`, and the CLI stage commands used by Airflow.

Silver writes deduplicated, validated Parquet per dataset with quarantined rejects and checksums. Gold writes `dim_customer`, `dim_subscription`, `fact_payments`, and `fact_service_events`; facts are partitioned by month and published through `gold/_committed.json` after all objects verify. Previous committed runs stay readable, so an interrupted build cannot corrupt the current dataset. Run summaries and all eight metric results are written to `s3://aster-local-artifacts/runs/<run-id>/`. Metric definitions, attribution rules, and quality gates are documented in [docs/metrics.md](docs/metrics.md).

`make query` prints the eight metrics from the committed gold dataset with DuckDB reading Parquet directly from MiniStack. Metric SQL lives in `sql/analytics/` and is exercised against hand-computed fixtures in `tests/unit/test_metrics_fixtures.py`.

## Orchestration

`make airflow-up` builds the pinned Airflow image, initializes the `airflow` database and metadata (never dropping existing data), and starts the API server, scheduler, and DAG processor on loopback. The UI listens at `http://127.0.0.1:8080`; the admin password is `AIRFLOW_ADMIN_PASSWORD` from `.env` and user `admin`. `make airflow-run` executes the DAG with `airflow dags test`, and `make test-airflow` asserts registration, a successful run, and failure propagation.

The DAG runs `extract_sources → validate_raw → transform_silver → validate_silver → build_gold → validate_gold → publish_run`, with two retries per task. A failing validation stops the chain before publication and the DAG run is marked failed. For a windowed backfill pass `--conf '{"window_start": "2025-03-01", "window_end": "2025-03-31"}'`; the windowed gold dataset is written under `gold/backfills/<run-id>/` and is deliberately not promoted to `gold/_committed.json`.

## Reliability laboratory

`make test-failure` runs the five M5 scenarios against real PostgreSQL, MiniStack, and the pipeline:

- Extraction interrupted by SIGKILL after the data write and after the manifest commit.
- Duplicate replay of committed batches with unchanged silver/gold results.
- Invalid source rows quarantined with reasons and without metric corruption.
- Gold transformation interrupted before publication, leaving the previous committed dataset queryable.
- Windowed backfill compared with a clean rebuild for date-local metrics and the trailing seven-day trend.

Each test asserts committed data, object keys, row counts, quarantine reasons, or metric values; exit codes alone are never treated as evidence. Details and known limitations are in [docs/checklist.md](docs/checklist.md).

## Monitoring

`make monitoring-up` starts pinned Grafana, Loki, Tempo, Prometheus, and Pushgateway containers with persistent volumes; `make monitoring-check` verifies readiness from the host. Pipeline runs push JSON logs to Loki, OTLP spans to Tempo, and run metrics through the Pushgateway to Prometheus. Grafana at `http://127.0.0.1:3000` has provisioned datasources and the `Aster Pipeline` dashboard; the admin password is `GF_SECURITY_ADMIN_PASSWORD` in `.env`. Airflow tasks emit the same telemetry through the Compose network. Loki and Tempo are distroless, so readiness is verified externally rather than by container health checks. See [docs/observability.md](docs/observability.md).

## Lifecycle and reset

`make down` stops services without removing named volumes. PostgreSQL initializes its database and user only when its volume is empty; changing the password in `.env` later does not change the existing database password. MiniStack snapshots metadata on graceful shutdown and stores S3 bytes in a separate volume; abrupt termination may lose unsnapshotted metadata.

`make clean` asks for confirmation before deleting **all PostgreSQL and MiniStack data**. It retains Terraform state; after a reset, run `make up`, wait for readiness, then `make infra-apply` to reconcile it. Terraform destroy only deletes empty buckets. Delete unwanted objects explicitly before destroying infrastructure. No routine restart clears persistent state.

See [checklist](docs/checklist.md). `make monitoring-down` stops the monitoring stack without deleting its volumes. Athena results are not evidence of AWS Athena compatibility or performance.
