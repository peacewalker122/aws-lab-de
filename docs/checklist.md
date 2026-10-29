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
- [x] Terraform plan with unchanged inputs reports no changes (re-verified in this milestone).

## M2: ingestion

- [x] Source tables, deterministic synthetic business data, and ingestion CLI.
- [x] Composite cursors, transactional source clock, raw Parquet, checksums, manifests, and checkpoints.
- [x] Commit marker is the raw manifest; the checkpoint advances only after the manifest and data object are verified.
- [x] Integration tests cover the manifest contract, replay after checkpoint rollback, updated rows, interruption between manifest and checkpoint, cursor ties, and concurrent writers with separate connections.
- [x] Concurrent writers are serialized by the source table lock and transactional cursor clock; the latest captured version per business key matches the source row exactly.
- [x] Replay of committed batches produces identical object keys and manifests and no duplicate logical rows.

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

## M5: reliability laboratory

All five scenarios run against real PostgreSQL and MiniStack and assert committed data, not exit codes.

- [x] Scenario A - extraction interruption: `test_scenario_a_interruption_after_data_write` and `test_scenario_a_interruption_after_manifest_commit`. A SIGKILL between the data write and the manifest leaves no committed batch; a SIGKILL after the manifest but before the checkpoint commit replays the same batch id with identical object keys. No loss or duplication.
- [x] Scenario B - duplicate replay: `test_scenario_b_duplicate_replay_preserves_results`. Resetting checkpoints and re-extracting every dataset produces identical raw keys, silver row counts, and all eight metrics.
- [x] Scenario C - invalid source data: `test_scenario_c_invalid_source_rows_are_quarantined`. Blank names, invalid plan status, negative amounts, succeeded payments without `paid_at`, negative durations and out-of-range status codes are quarantined with reasons; accepted rows and metrics are unchanged.
- [x] Scenario D - transformation interruption: `test_scenario_d_interrupted_gold_keeps_previous_commit`. A SIGKILL before the gold commit pointer leaves the previous committed dataset byte-identical and queryable; the partial run is visible but unpublished; a later run republishes identical metrics.
- [x] Scenario E - backfill: `test_scenario_e_backfill_matches_clean_rebuild`. A windowed rebuild matches a clean full rebuild for date-local metrics and for the seven-day trend where the trailing window is fully inside the range; the committed pointer is not moved by a windowed backfill.

### Monitoring stack

- [x] Real, pinned Grafana 13.2.5, Loki 3.6.19, Tempo 2.10.10, Prometheus v3.15.1 and Pushgateway v1.11.5 with persistent volumes.
- [x] Loopback-only ports and admin credentials generated into the untracked `.env`; no secrets committed.
- [x] Grafana datasources and the `aster-pipeline` dashboard provisioned from files.
- [x] Pipeline pushes JSON logs to Loki, OTLP spans to Tempo, and run metrics through the Pushgateway to Prometheus.
- [x] `make monitoring-check` verifies readiness; `tests/integration/test_observability.py` asserts logs, metrics and traces actually arrived, and that the dashboard is provisioned.
- [x] Operation, retention, and credentials documented in `docs/observability.md`.

## Known limitations and blockers

- Athena coverage is still a constant query only; Glue/Athena lake queries remain unverified in MiniStack. DuckDB is the analytical engine for all metrics.
- No real AWS deployment has been performed and none is claimed. Every result is local emulation measured on this machine.
- Loki and Tempo images are distroless: they cannot self-report Docker health checks. Readiness is validated externally via `/ready` (`make monitoring-check`, observability tests).
- The Pushgateway intentionally keeps only the latest pipeline metrics group; historical trends come from the artefacts in the artifacts bucket, not from Prometheus.
- Extraction takes a `SHARE` table lock per batch. This protects cursor correctness at the cost of pausing writes during a batch; throughput was not benchmarked or claimed.
- Terraform state, `.env`, and `.local/` are untracked; no credentials, state, or benchmark numbers are committed.

## M6: portfolio packaging

Not implemented: architecture diagram, ADRs, measured benchmark report, case study, AWS migration plan, cost model, and demonstration script.
