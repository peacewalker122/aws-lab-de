# Limitations and boundaries

Aster is a local proof of concept. This page records the limits that are intentional, the limits that are temporary, and the evidence behind each statement. No AWS deployment, AWS performance, or AWS scalability is claimed.

## Local-only endpoints and credentials

- `Config.from_env()` rejects any `AWS_ENDPOINT_URL`, observability endpoint, or optional URL that is not `http` on a loopback host or a local Compose service name with an explicit port. MiniStack cannot be replaced by a real AWS endpoint through configuration alone.
- All S3 traffic uses the static credentials `test` / `test`; Terraform uses dummy credentials. MiniStack does not enforce IAM, bucket policies, or least privilege, so the repository demonstrates IAM intent only. The production migration plan must supply real IAM roles.
- PostgreSQL, MiniStack, Airflow, Grafana, Loki, Tempo, Prometheus, and the Pushgateway publish only on loopback addresses. Credentials and endpoints live in the untracked `.env`; `scripts/local_secrets.py` creates missing secrets once and never rewrites existing values. Compose fails explicitly when the Grafana or Airflow admin password variable is missing.
- The Grafana container receives only `GF_SECURITY_ADMIN_PASSWORD`; it does not receive the PostgreSQL password, the Airflow admin password, or the Airflow metadata DSN.
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
- The lock protects whole `pipeline`/`backfill` runs and individual CLI stage commands. A sequence of separate stage commands (for example `make transform`) releases the lock between stages; the publication fence, not the lock, is what prevents such a run from publishing another run's gold output. A fenced run fails instead of silently publishing foreign output and must be retried.
- A killed process releases the advisory lock when its connection closes. Partially written objects remain in the lake under run-specific prefixes but are never published.
- The lock supplies mutual exclusion, not fairness or a queue. A competing run waits indefinitely and fails only if its connection or statement times out.

## Semantics and data handling

- Empty source tables and empty backfill windows are valid: every dataset publishes a zero-row Parquet object, and metrics return no rows instead of failing.
- A windowed gold build publishes dimensions rebuilt from the full committed silver snapshot, so a backfill reprocesses dimensions as well as the requested facts.
- The seven-day revenue trend depends on a build-time calendar (`revenue_trend_seed`). A backfill includes six lookback days of revenue so the trend matches a full rebuild, but lookback rows are never returned as trend output. Calendar bounds and lookback revenue come from the committed silver payments, while in-window revenue comes from the gold fact table. If a successful silver payment were dropped by a gold join, the calendar would still cover that date and the trend would report zero revenue for it; the full rebuild and a backfill would agree, because both use the same silver-derived calendar and the same gold-derived revenue.
- Cursor timestamps in manifests are UTC ISO-8601. Ordering and monotonicity checks parse the instant; a summary comparison of timestamp strings is not valid across offsets.
- DDL and seed operations are not concurrent-safe by design: `seed` requires all source tables to be empty and refuses to modify existing data, and migrations run inside a transaction but assume a single writer.

## Emulator coverage

- MiniStack is verified for S3 object operations and a constant Athena query only. Glue/Athena lake queries remain unverified; DuckDB is the analytical engine for every metric.
- Terraform provisions buckets only. There is no emulated IAM enforcement, no Glue catalog registration, and no Athena table definition.
- Loki and Tempo images are distroless, so container health checks are unavailable and readiness is verified externally.
