# Implementation checklist

## M0: compatibility spike

- [x] Scaffold MiniStack Full, Terraform, and local smoke tests.
- [x] Verify MiniStack startup and health.
- [x] Terraform init and provision one S3 bucket.
- [x] Verify S3 object upload and retrieval.
- [x] Verify DuckDB reads and queries a local Parquet fixture.
- [x] Probe Athena query API separately and record the result.
- [x] Record compatibility evidence and decision in `docs/compatibility.md`.

## M1: infrastructure foundation

- [x] Add PostgreSQL with persistent storage and a readiness check.
- [x] Provision raw, silver, gold, and artifact buckets.
- [x] Configure local credentials and service lifecycle commands.
- [x] Verify fresh initialization and persistence across restart: isolated Compose project, Terraform apply and unchanged plan, PostgreSQL row and S3 object survived restart.

## M2: ingestion

- [x] Source tables, deterministic synthetic business data, and ingestion CLI.
- [x] Composite cursors, transactional source clock, raw Parquet, checksums, manifests, and checkpoints.
- [x] Commit marker is the raw manifest; the checkpoint advances only after the manifest and data object are verified.
- [x] Integration tests cover manifest contract, replay after checkpoint rollback, updated rows, interruption between manifest and checkpoint, cursor ties, and concurrent writers with separate connections.
- [x] Concurrent writers are serialized by the source table lock and transactional cursor clock; the latest captured version per business key matches the source row.
- [x] Replay of committed batches produces identical object keys and manifests.

Unverified at this milestone: no separate-process termination, transformation recovery, or backfill coverage yet.

## M3: analytical modeling

- [x] Silver deduplication by business key with UTC normalization and decimal preservation.
- [x] Quarantine of malformed records with reject reasons; accepted + rejected reconciles to distinct raw keys.
- [x] Gold dimensions and facts with deterministic surrogate keys and month partitions for facts.
- [x] Atomic publication through `gold/_committed.json`; prior runs remain readable.
- [x] Eight DuckDB metrics in `sql/analytics/` with hand-computed fixture tests.
- [x] Quality gates for raw, silver, and gold stages with nonzero exit on failure.
- [x] Metric semantics documented in `docs/metrics.md`.
- [x] Integration test runs the complete pipeline against PostgreSQL and MiniStack and asserts row counts, checksums, artifacts and all eight metrics.

## M4: orchestration

- [x] Pinned Airflow 3.1.0 image with project dependencies and preinstalled DuckDB `httpfs`.
- [x] DAG `aster_pipeline` with extract, raw validation, silver, silver validation, gold, gold validation, and publication tasks.
- [x] LocalExecutor services: init/migrate, API server, scheduler, and DAG processor with health checks.
- [x] Two retries per task with a delay; downstream tasks are skipped when a quality gate fails.
- [x] Manual runs through `airflow dags test` and the API UI; run IDs map to artifact prefixes.
- [x] Windowed gold builds for backfill through `--conf '{"window_start": ..., "window_end": ...}'`; windowed outputs are not promoted to the committed pointer.
- [x] Structured JSON pipeline logs and per-stage artifact records.
- [x] Airflow integration tests cover DAG registration, a successful end-to-end run, and failure propagation.

Safety note: the `airflow` database is created in the existing PostgreSQL instance with `CREATE DATABASE`; no volumes or existing databases are dropped.

## M5 through M6

Not implemented at this commit: the five reliability scenarios and the local LGTM monitoring stack. Portfolio packaging remains open.

Athena coverage remains limited to a constant query; Glue integration remains unverified.
