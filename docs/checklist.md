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
- [x] Atomic publication through `gold/_committed.json`; prior runs remain readable. Superseded by the staged-pointer gate described under "Independent re-review remediation" (F1).
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
- [x] Windowed gold builds for backfill through `--conf '{"window_start": ..., "window_end": ...}'`; the validated windowed dataset is published to `gold/backfills/<run-id>/manifest.json` and is never promoted to the committed pointer.
- [x] Structured JSON pipeline logs and per-stage artifact records.
- [x] Airflow integration tests cover DAG registration, a successful end-to-end run in an isolated schema and buckets, a real quality-gate failure that blocks publication, and the Airflow password file mode. Tests never write to the primary schema or the `aster-local-*` buckets.

## M5: reliability laboratory

All five scenarios run against real PostgreSQL and MiniStack and assert committed data, not exit codes.

- [x] Scenario A - extraction interruption: `test_scenario_a_interruption_after_data_write` and `test_scenario_a_interruption_after_manifest_commit`. A SIGKILL between the data write and the manifest leaves no committed batch; a SIGKILL after the manifest but before the checkpoint commit replays the same batch id with identical object keys. No loss or duplication.
- [x] Scenario B - duplicate replay: `test_scenario_b_duplicate_replay_preserves_results`. Resetting checkpoints and re-extracting every dataset produces identical raw keys, silver row counts, and all eight metrics.
- [x] Scenario C - invalid source data: `test_scenario_c_invalid_source_rows_are_quarantined`. Blank names, invalid plan status, negative amounts, succeeded payments without `paid_at`, negative durations and out-of-range status codes are quarantined with reasons; accepted rows and metrics are unchanged.
- [x] Scenario D - transformation interruption: `test_scenario_d_interrupted_gold_keeps_previous_commit`. A SIGKILL after the staged pointer write and before any publication leaves the previous committed dataset byte-identical and queryable; the staged run is visible but unpublished; a later run republishes identical metrics.
- [x] Scenario E - backfill: `test_scenario_e_backfill_matches_clean_rebuild`. Four windows (single leading day, a day with a full lookback, an interior range, and a trailing range) are compared with the full rebuild for all eight metrics on every date in the window. Gold publishes a `revenue_trend_seed` calendar carrying six lookback days of revenue so the trend matches the full rebuild at the window start; lookback rows are never returned. The committed pointer is not moved by a windowed backfill, and the published `runs/<run-id>/metrics.json` artifact is asserted to equal the metrics of the published backfill pointer.

### Monitoring stack

- [x] Real, pinned Grafana 13.2.5, Loki 3.6.19, Tempo 2.10.10, Prometheus v3.15.1 and Pushgateway v1.11.5 with persistent volumes.
- [x] Loopback-only ports and admin credentials generated into the untracked `.env`; no secrets committed.
- [x] Grafana datasources and the `aster-pipeline` dashboard provisioned from files.
- [x] Pipeline pushes JSON logs to Loki, OTLP spans to Tempo, and run metrics through the Pushgateway to Prometheus.
- [x] `make monitoring-check` verifies readiness; `tests/integration/test_observability.py` asserts logs, metrics and traces actually arrived, and that the dashboard is provisioned.
- [x] Operation, retention, and credentials documented in `docs/observability.md`.

## Review remediation evidence

Independent review of M2-M5 found seven defects. Each is fixed and covered by a test.

- [x] Empty fact and dataset outputs: a zero-row partitioned `COPY` produced an empty directory. Partitioning now applies only when rows exist (`fix(transform): write valid Parquet for empty fact and dataset outputs`). Evidence: `tests/integration/test_empty_data.py` runs a full pipeline over empty source tables and an empty-window backfill, asserts readable zero-row Parquet objects, and asserts the committed pointer is unchanged.
- [x] Hermetic Airflow tests: each test seeds an isolated PostgreSQL schema and four isolated buckets, passes them into the container, disables telemetry, and asserts artifacts there. The failure test corrupts a committed raw object so `validate_raw` genuinely fails, then asserts no gold object, no committed pointer, no `publish_run` stage and no success artifact (`test(airflow): run DAG tests in an isolated schema and buckets`). Verified that the primary schema and `aster-local-*` buckets were not mutated.
- [x] Backfill seven-day trend: the windowed trend now carries six lookback days and matches the full rebuild at both boundaries; tested for every date in four windows (`fix(analytics): make backfilled seven-day revenue trend match a full rebuild`). Empty windows are tested in `tests/integration/test_empty_data.py`.
- [x] Windowed gold validation: fact reconciliation now compares gold counts with window-filtered committed silver counts and reconciles lookback revenue against silver; a deliberately mismatched window fails the check (`fix(quality): reconcile windowed gold against window-filtered silver`).
- [x] Credential isolation: Grafana receives only `GF_SECURITY_ADMIN_PASSWORD` through Compose interpolation, never `.env`; the Airflow simple-auth password file is written with mode `0600` (`fix(security): isolate Grafana credentials and lock down the Airflow password file`). Evidence: `tests/unit/test_compose_secrets.py` and `test_airflow_admin_password_file_is_owner_only`. Existing untracked secrets and named volumes were preserved.
- [x] Overlapping pipelines: every run, backfill, and CLI stage holds a schema-scoped PostgreSQL advisory lock, and publication requires the build record, silver snapshot, and committed gold pointer to belong to the publishing run (`feat(pipeline): serialize pipeline runs and fence publication`). Evidence: `tests/integration/test_concurrency.py` asserts disjoint stage intervals for two concurrent runs and both fence rejections.
- [x] Cursor timezones: manifest cursors are written as UTC instants, ordering parses the instant, and checkpoint reads use `AT TIME ZONE 'UTC'` (`fix(ingestion): compare and publish cursors as UTC instants`). Evidence: `tests/unit/test_cursors.py` and `test_manifest_cursors_stay_ordered_for_non_utc_source_sessions`.
- [x] Explicit documentation of the local-only endpoint and credential boundary, the full-lake verification cost, and the source timestamp repurposing in `docs/limitations.md`.

## Independent re-review remediation (F1-F5)

Second independent review found five defects. Each is fixed, covered by tests, and documented in `docs/limitations.md` and `docs/metrics.md`.

- [x] F1 - publication before quality. `build_gold` used to write `gold/_committed.json` (or a backfill manifest) before `validate_gold` ran, so a failed quality gate left invalid data queryable and publishable. `build_gold` now writes only `gold/_staged/<run-id>.json`; `validate_gold` re-reads that staged pointer, validates its objects, and records a passing gate with a digest of the pointer; `publish_run` refuses without the gate, refuses when the staged pointer changed after validation, and only then performs the single pointer write (`fix(transform): publish gold only after the quality gate passes`). Evidence: `tests/integration/test_gold_publication.py` asserts the staged pointer and unchanged committed pointer after a build, a corrupted staged object failing `gold.<table>.checksums` and blocking publication while the previous committed dataset stays queryable and byte-identical, a CLI sequence (`build-gold`, direct `publish-run`, `validate-gold`, `publish-run`) where direct publication is refused with "no passing gold validation", and a backfill that publishes its manifest only after validation. `tests/integration/test_airflow.py::test_gold_quality_gate_blocks_staged_output_in_airflow` runs the same path inside the Airflow container. `tests/integration/test_concurrency.py` keeps the ownership fences: a stale-silver build and a build that lost the publication race both fail and write no success artifact.
- [x] F2 - mutable silver reference during validation. Validation re-read the committed silver pointer, so silver advancing between stages silently changed the reference and equal-count value changes were invisible. The staged gold pointer now embeds the exact silver pointer used for the build; reconciliation uses that immutable snapshot and a `gold.silver_snapshot_current` check fences a superseded build, with publication repeating the provenance check (`fix(quality): validate gold against the silver snapshot it was built from`). Evidence: `test_gold_validates_against_the_silver_snapshot_it_was_built_from` publishes a newer silver snapshot with identical row counts but incremented payment amounts, asserts fact reconciliation still passes against the embedded snapshot, asserts only the snapshot fence fails, and asserts no publication; `test_stale_gold_build_fails_validation_after_silver_advances` and `test_publish_is_fenced_when_silver_advances_after_validation` cover both interleaving orders.
- [x] F3 - legacy gold without `revenue_trend_seed`. `quality.validate_gold` now checks the pointer contract (`run_id`, `silver_run_id`, embedded `silver`, `trend_seed`, four tables) before anything else and fails with the missing keys plus an explicit rebuild command instead of raising `KeyError` or passing partially. Legacy full pointers remain queryable because the fallback calendar equals a full-build seed; legacy windowed pointers raise an actionable error instead of returning a trend on a wrong calendar (`fix(quality): report legacy gold pointers with a rebuild remedy`). Evidence: `tests/integration/test_legacy_gold.py` asserts all eight metrics are unchanged for a legacy pointer, that no gold object is deleted, that validation reports `legacy: true` with the rebuild remedy, that a rebuild republishes in place and then validates, and that a legacy windowed pointer is refused with its rebuild command.
- [x] F4 - over-broad service environment. Airflow containers and PostgreSQL loaded all of `.env`, including the Grafana admin secret. Compose now interpolates only `POSTGRES_PASSWORD` into PostgreSQL and `POSTGRES_PASSWORD` plus `AIRFLOW_ADMIN_PASSWORD` into the Airflow services; mounts, ports, commands, entrypoint and the `0600` password file are unchanged (`fix(security): give each service only the secrets it needs`). Evidence: `tests/unit/test_compose_secrets.py` asserts the raw compose blocks, the rendered `docker compose config`, the entrypoint's log-free secret handling, and the environment of the running Airflow scheduler container (`GF_SECURITY_ADMIN_PASSWORD` absent, both required variables present). The Airflow services were recreated and the full `airflow` marker suite stayed green.
- [x] F5 - ambiguous cursor timestamps and missing regressions. `lake.cursor_end` now parses through `lake.parse_instant`, which rejects timestamps without a UTC offset instead of interpreting them in the host timezone (`fix(ingestion): reject manifest cursors without a UTC offset`). Evidence: `tests/unit/test_cursors.py` covers naive and date-only cursors; `tests/integration/test_ingestion.py` asserts a naive manifest fails raw validation and that `customers/` listing never includes `customers_archive/`; `tests/failure/test_scenarios.py::test_scenario_e_backfill_matches_clean_rebuild` now asserts the published `runs/<run-id>/metrics.json` artifact equals the metrics of the published backfill pointer and the clean-rebuild window reference.

### Evidence after the remediation

Marker-separated suite on this host, each batch run in its own process: `not integration` 28 passed, integration without Airflow/observability/failure 27 passed, `failure` 6 passed, `airflow` 5 passed, `observability` 2 passed. `ruff check src tests orchestration` is clean.

## Known limitations and blockers

- Athena coverage is still a constant query only; Glue/Athena lake queries remain unverified in MiniStack. DuckDB is the analytical engine for all metrics.
- No real AWS deployment has been performed and none is claimed. Every result is local emulation measured on this machine.
- Loki and Tempo images are distroless: they cannot self-report Docker health checks. Readiness is validated externally via `/ready` (`make monitoring-check`, observability tests).
- The Pushgateway intentionally keeps only the latest pipeline metrics group; historical trends come from the artefacts in the artifacts bucket, not from Prometheus.
- Extraction takes a `SHARE` table lock per batch. This protects cursor correctness at the cost of pausing writes during a batch; throughput was not benchmarked or claimed.
- Raw validation checksums and reconciles every committed raw object each run, and silver/gold rebuilds read the full committed snapshot. Cost grows with total history, not with the incremental slice; no pruning or incremental silver maintenance exists. See `docs/limitations.md`.
- Pipeline serialization is a PostgreSQL advisory lock scoped to the schema. A sequence of separate stage commands releases the lock between stages; the publication gate and fences, not the lock, decide whether such a run may publish, and a fenced run fails and must be retried.
- Publication is a single-object pointer write, not a transaction across objects. A crash after the pointer write but before `run.json`/`metrics.json` leaves a valid published dataset with missing artifacts; the missing artifact is the detectable signal. A crash before the pointer write leaves the previous pointer and the staged objects in place.
- The publication ordering fence compares host wall-clock timestamps of staged builds; it is not clock-skew safe across machines, and two builds of the same silver cannot publish out of order without a rebuild.
- Staged pointers (`gold/_staged/<run-id>.json`) and superseded run prefixes are retained like every other run prefix; there is no pruning.
- Gold published before the snapshot/seed contract can still be queried but cannot pass validation until it is rebuilt; legacy windowed pointers must be rebuilt before their trend can be read.
- Containers created before the environment narrowing keep the old environment until they are recreated (`make airflow-up`).
- Source `updated_at` and `ingested_at` are trigger-assigned commit timestamps repurposed as ingestion cursors. Writes that bypass or disable the triggers invalidate cursor guarantees; deletes are not detected; ordering is per dataset. See `docs/limitations.md`.
- Terraform state, `.env`, and `.local/` are untracked; no credentials, state, or benchmark numbers are committed.
- The whole suite passes when run per marker in separate processes: `not integration` 28, integration without Airflow/observability/failure 27, `failure` 6, `airflow` 5, `observability` 2. A single-process run of every test on this host is still not verified. During this remediation the user-level `podman-compose@infrastructure.service` unit stopped MiniStack, the monitoring stack and PostgreSQL mid-batch (`received smart shutdown request`, containers exited 0, PostgreSQL left unhealthy), which produced fixture errors rather than assertion failures; after restarting the stack the same marker batches were green. This is a host memory/runtime limit, not a data-correctness defect.

## M6: portfolio packaging

Not implemented: architecture diagram, ADRs, measured benchmark report, case study, AWS migration plan, cost model, and demonstration script.
