# Implementation checklist

## M0: compatibility spike

- [x] Scaffold MiniStack Full, Terraform, and local smoke tests.
- [x] Verify MiniStack startup and health.
- [x] Terraform init and provision one S3 bucket.
- [x] Verify S3 object upload and retrieval.
- [x] Verify DuckDB reads and queries a local Parquet fixture.
- [x] Probe Athena query API separately and record the result.
- [x] Record compatibility evidence and decision in `docs/compatibility.md`.

M1 through M6 remain unimplemented. Athena coverage is limited to a constant query; Glue integration remains unverified.
