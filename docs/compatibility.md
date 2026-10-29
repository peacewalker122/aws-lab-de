# M0 compatibility evidence

MiniStack's [configuration documentation](https://ministack.org/docs/configuration) describes its Full image, readiness endpoint, persistence controls, and DuckDB-backed Athena. Its [Terraform guide](https://ministack.org/docs/iac) documents AWS provider endpoint overrides.

Local run: Fedora, Podman 5.8.4 with podman-compose 1.6.0, MiniStack 1.5.33-full, Terraform 1.11.4, Python 3.12.11, AWS provider 5.100.0.

| Capability | Local result |
| --- | --- |
| MiniStack health | Passed: Full edition and S3, Athena, Glue reported available |
| Terraform S3 bucket | Passed: apply created one bucket; repeated plan reported no changes |
| S3 upload and retrieval | Passed: integration test compared uploaded bytes and removed its object |
| DuckDB query over Parquet | Passed: fixture sum was 60 |
| Athena query API | Passed for `SELECT 42`: start, poll, and results returned 42 |
| Graceful restart | Bucket persisted; Terraform plan remained unchanged and S3 roundtrip passed |

Decision: proceed with local S3 and DuckDB for the next milestone. Athena's constant query is only a narrow API check, not verification of catalog integration, querying lake data, AWS parity, or production use. Glue behavior remains untested. No real AWS behavior, scalability, or cost has been measured.
