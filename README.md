# Aster Data Platform

Aster is a local AWS-compatible proof of concept for a fictional telecom/SaaS business. Incremental ingestion into the raw lake zone is implemented and tested; analytical models, orchestration, recovery validation, and monitoring are not implemented yet. See [project.md](project.md) for requirements and milestones.

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

After provisioning, `make migrate` initializes source tables and cursor triggers. `make seed` inserts a synthetic small dataset only when all source tables are empty; set `ASTER_SEED_SIZE` to `small`, `medium`, or `large` and `ASTER_SEED` to an integer to vary it. `make ingest` extracts batches into the raw bucket; `ASTER_BATCH_SIZE` controls batch size. Set overrides in `.env`. Ingestion is not yet a complete data pipeline.

Source triggers assign monotonically increasing cursor timestamps using a transactional clock row. Extraction takes a shared table lock for each batch, then advances its PostgreSQL checkpoint only after verifying S3 data and manifest. This serializes writes during each batch and trades throughput for correctness. Source deletes and event updates are rejected; disabling triggers or writing outside this contract invalidates cursor guarantees. Raw batches published before a checkpoint rollback are replayed idempotently with identical object keys and manifests. Integration tests cover concurrent writers, cursor ties, and interrupted checkpoint commits. Downstream deduplication and transformation recovery are not implemented yet.

## Lifecycle and reset

`make down` stops services without removing named volumes. PostgreSQL initializes its database and user only when its volume is empty; changing the password in `.env` later does not change the existing database password. MiniStack snapshots metadata on graceful shutdown and stores S3 bytes in a separate volume; abrupt termination may lose unsnapshotted metadata.

`make clean` asks for confirmation before deleting **all PostgreSQL and MiniStack data**. It retains Terraform state; after a reset, run `make up`, wait for readiness, then `make infra-apply` to reconcile it. Terraform destroy only deletes empty buckets. Delete unwanted objects explicitly before destroying infrastructure. No routine restart clears persistent state.

See [checklist](docs/checklist.md). Athena results are not evidence of AWS Athena compatibility or performance.
