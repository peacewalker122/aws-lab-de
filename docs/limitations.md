# Limitations and boundaries

Aster is a local proof of concept. This page records the limits that are intentional, the limits that are temporary, and the evidence behind each statement. No AWS deployment, AWS performance, or AWS scalability is claimed.

## Local-only endpoints and credentials

- `Config.from_env()` rejects any `AWS_ENDPOINT_URL`, observability endpoint, or optional URL that is not `http` on a loopback host or a local Compose service name with an explicit port. MiniStack cannot be replaced by a real AWS endpoint through configuration alone.
- All S3 traffic uses the static credentials `test` / `test`; Terraform uses dummy credentials. MiniStack does not enforce IAM, bucket policies, or least privilege, so the repository demonstrates IAM intent only. The production migration plan must supply real IAM roles.
- PostgreSQL, MiniStack, Airflow, Grafana, Loki, Tempo, Prometheus, and the Pushgateway publish only on loopback addresses. Credentials and endpoints live in the untracked `.env`; `scripts/local_secrets.py` creates missing secrets once and never rewrites existing values. Compose fails explicitly when the Grafana or Airflow admin password variable is missing.
- Containers receive only the variables they need. The Grafana container receives `GF_SECURITY_ADMIN_PASSWORD`; it does not receive the PostgreSQL password, the Airflow admin password, or the Airflow metadata DSN. PostgreSQL receives only `POSTGRES_PASSWORD`, and the Airflow services receive only `POSTGRES_PASSWORD` and `AIRFLOW_ADMIN_PASSWORD` from `.env`; no service loads the whole file through `env_file`. The Airflow entrypoint writes the simple-auth password file with mode `0600` and never logs a secret.
- Data is synthetic. No production credentials, customer data, or deployed resources are used.

## Throughput is not measured

No benchmark has been executed. Every timing in logs and artifacts is an observed local duration for the synthetic dataset, not a capacity claim. Known structural costs:

- **Full-lake raw verification:** `quality.validate_raw` reads and checksums every committed raw object on every run, and reconciles counts against `count(*)` of the source tables. Work grows with total raw history, not with the new batch, and the reconciliation needs a live source connection.
- **Full snapshot rebuilds:** silver and gold are rebuilt from the entire committed silver snapshot each run. `build_silver` reads all raw manifests, and `build_gold` reads every committed silver object. There is no partition pruning or incremental silver maintenance, so runtime and DuckDB memory grow with total data volume.
- **Share lock per batch:** extraction takes `LOCK TABLE ... IN SHARE MODE` for each batch, which pauses source writes while the batch is extracted. This protects cursor correctness and deliberately trades throughput for correctness; throughput was not benchmarked.
- Windowed backfills still read the full committed silver snapshot to build dimensions and the trend lookback, so a narrow window does not make a backfill cheap.
- Quality gates open their own DuckDB connections and rescan the raw, silver, and gold objects, so validation adds several full scans to every run.

The large dataset (1,000,000 events) is not exercised by any automated test.

## Source timestamp repurposing

The source tables use application timestamps as ingestion cursors:

- `customers`, `subscriptions`, `payments`: the composite cursor is `(updated_at, <primary key>)`. Triggers in `sql/migrations/001_source.sql` assign `updated_at` from a transactional clock row, so the column is a commit timestamp rather than the business update time. A source write must go through these triggers.
- `service_events`: the cursor is `(ingested_at, event_id)` while `event_time` is the business event time. `ingested_at` is also a trigger-assigned commit timestamp; the seed sets it equal to `event_time` for convenience. Event-time attribution for metrics uses `event_time`, not `ingested_at`.
- Consequences: a row whose cursor column is rewritten to an older value is skipped; writes that bypass or disable the triggers invalidate cursor guarantees; source-side deletes are not detected; events are append-only and rejected on update. Because the cursor is the commit clock, a late business event is captured when it is committed, not when it occurred, and ordering is per dataset, not across datasets.

## Concurrency and publication

- Serialization uses a PostgreSQL advisory lock keyed by `aster-pipeline:<schema>`. It serializes Aster runs in one PostgreSQL database and schema. Two environments with different schemas, or two different databases, can run concurrently and are isolated by their own buckets and schema.
- The lock protects whole `pipeline`/`backfill` runs and individual CLI stage commands. A sequence of separate stage commands (for example `make transform`) releases the lock between stages, so the publication gate and fences, not the lock, decide whether such a run may publish. A fenced run fails instead of silently publishing foreign or stale output and must be retried.
- Silver publication and gold publication are two-phase contracts, one for each zone. `build_silver` writes only `silver/_staged/<run-id>.json`; `validate_silver` validates that exact pointer and records a passing gate over a digest of the pointer's content; `publish_silver` refuses without the gate, refuses when the staged content changed after validation, refuses when a pointer staged later is already committed, re-verifies every silver object's row count and checksum, and then performs the single-object write to `silver/_committed.json`, embedding the validation record. `build_gold` writes only `gold/_staged/<run-id>.json`; `validate_gold` re-reads that staged pointer, validates its objects, and records a passing gate with a digest of the pointer. `publish_run` refuses without the gate, refuses when the staged pointer changed after validation, re-verifies every gold object and the embedded silver snapshot after calculating metrics and immediately before writing `gold/_committed.json` or `gold/backfills/<run-id>/manifest.json`. Each checksum manifest must cover exactly the listed objects. There is no rename or transaction across objects: the pointer write is the commit, and a crash before it leaves the previous pointer and the staged objects in place.
- `build_gold` refuses committed silver without a validation record that matches its exact content, and re-reads the silver snapshot bytes and checksums before building, so gold cannot come from failed, ungated, or post-validation-modified silver. Publication repeats the same verification for the embedded snapshot.
- PostgreSQL advisory locks serialize cooperating Aster runs and individual CLI stages that share one database and schema. The lock does not cover the object store, writers using another database/schema, or the gaps between separately invoked CLI stages; publication gates and ownership fences reject stale or interleaved Aster output. Object checksums and row counts are re-read after metric calculation immediately before the pointer write. The object adapter does not issue conditional writes or compare-and-swap requests, so an out-of-band writer can still change an object in the short interval between that final read and pointer write. Persistent corruption is detected by a later build or validation; no multi-object transaction or protection from non-cooperating writers is claimed.
- Publication re-reads every gold object and the embedded silver snapshot, so a corruption that happens after a passing quality gate is blocked at publication time and the previous committed pointer and dataset stay untouched.
- If writing a run artifact fails after the pointer commit and the artifact store accepts the failure record, `runs/<run-id>/run.json` records `status: failed`, `gold_state: published`, the committed pointer, and the error. If the artifact store also rejects that failure record, the committed pointer remains authoritative and the run artifact may be absent. A SIGKILL in that window likewise cannot write a failure record. Multi-object atomicity is not fabricated.
- The staged pointer embeds the exact silver pointer used for the build. Validation reconciles against that immutable snapshot, and a `gold.silver_snapshot_current` check fails when newer silver was committed in between, with a rebuild instruction. Publication repeats the same provenance check, so a build that passed validation before silver advanced still cannot publish.
- A full rebuild may only publish over a committed pointer that is older than its own staged build, for silver and for gold. Two silver builds, or two gold builds on the same silver, therefore cannot publish out of order; the loser must rebuild. Timestamps are compared as UTC instants on one host; there is no cross-host clock-skew protection.
- A killed process releases the advisory lock when its connection closes. Partially written objects remain in the lake under run-specific prefixes, staged pointers included, but they are never published.
- The lock supplies mutual exclusion, not fairness or a queue. A competing run waits indefinitely and fails only if its connection or statement times out.

## Legacy silver pointers

- A committed silver pointer published before the staged gate lacks a `validation` record. Such silver stays readable and the existing gold dataset stays queryable, but `build_gold` refuses to use it and prints the rebuild sequence (`transform-silver`, `validate-silver`, `publish-silver`, or `make transform`). Nothing is deleted: the ungated pointer is replaced in place by the next gated publication.

## Legacy gold pointers

- Gold published before the `revenue_trend_seed` contract lacks the seed, the embedded silver snapshot, and `silver_run_id`. Such a committed pointer still queries correctly for all eight metrics: the fallback calendar spans the successful gold payment dates and carries no lookback rows, which is exactly what a full-build seed contains. Windowed legacy pointers are refused with an actionable error because their trend calendar cannot be reconstructed safely.
- Validation reports a legacy pointer with the missing keys and the rebuild command (`python -m aster --run-id <new-run-id> pipeline`, or `make transform` for a staged rebuild). Rebuilding republishes the committed pointer in place. Existing gold objects are never deleted; superseded run prefixes remain readable by run id.

## Semantics and data handling

- Empty source tables and empty backfill windows are valid: every dataset publishes a zero-row Parquet object, and metrics return no rows instead of failing.
- A windowed gold build publishes dimensions rebuilt from the full committed silver snapshot, so a backfill reprocesses dimensions as well as the requested facts.
- The seven-day revenue trend depends on a build-time calendar (`revenue_trend_seed`). A backfill includes six lookback days of revenue so the trend matches a full rebuild, but lookback rows are never returned as trend output. Calendar bounds and lookback revenue come from the committed silver payments, while in-window revenue comes from the gold fact table. If a successful silver payment were dropped by a gold join, the calendar would still cover that date and the trend would report zero revenue for it; the full rebuild and a backfill would agree, because both use the same silver-derived calendar and the same gold-derived revenue.
- Cursor timestamps in manifests are UTC ISO-8601. Ordering and monotonicity checks parse the instant; a summary comparison of timestamp strings is not valid across offsets, and a manifest whose cursor timestamp has no UTC offset is rejected instead of being interpreted in the host timezone.
- DDL and seed operations are not concurrent-safe by design: `seed` requires all source tables to be empty and refuses to modify existing data, and migrations run inside a transaction but assume a single writer.

## Emulator coverage

- MiniStack is verified for S3 object operations and a constant Athena query only. Glue/Athena lake queries remain unverified; DuckDB is the analytical engine for every metric.
- Terraform provisions buckets only. There is no emulated IAM enforcement, no Glue catalog registration, and no Athena table definition.
- Loki and Tempo images are distroless, so container health checks are unavailable and readiness is verified externally.
