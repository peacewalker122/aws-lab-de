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

## M3 through M6

Not implemented at this commit: analytical models, orchestration, reliability scenarios, and portfolio packaging.

Athena coverage remains limited to a constant query; Glue integration remains unverified.
