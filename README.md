# Aster Data Platform

Aster is a local AWS-compatible proof of concept for a fictional telecom/SaaS business. This repository currently contains only the M0 compatibility spike, not a working data pipeline or an AWS deployment. See [project.md](project.md) for requirements and milestones.

## Prerequisites

- Docker with Compose and a running container engine
- Terraform 1.9 or newer (below 2.0)
- uv and Python 3.12 or 3.13
- curl and make
- Network access to download the pinned image, Terraform provider and Python packages

The M0 Terraform configuration accepts only loopback endpoints and uses dummy credentials. Never use production credentials. It creates one local bucket, not the four M1 buckets.

## Run the spike

```sh
uv sync --locked
make up
make doctor
make infra-init
make infra-apply
make infra-plan
make test
make test-integration
make athena
```

Wait for MiniStack's health check before provisioning. If Terraform is installed through mise without activation, prefix Make commands with `mise exec terraform@1.11.4 --`. `make doctor` fails if MiniStack is not reachable. The integration test uploads, reads, and deletes one uniquely named object in the Terraform-managed bucket. `make athena` is a separate optional probe; an error means Athena has not been verified, not that it succeeded. To choose another local endpoint set `TF_VAR_endpoint` for Terraform and `AWS_ENDPOINT_URL` for Python; set `TF_VAR_environment` and `ASTER_BUCKET` consistently if changing the bucket name. `.env.example` documents Python defaults; export settings in your shell, it is not automatically loaded.

`make down` stops services without removing volumes. MiniStack snapshots service state on graceful shutdown and stores S3 bytes in named volumes. Abrupt termination may lose unsnapshotted metadata. `make clean` asks for confirmation and deletes emulator volumes, including all stored data, but retains Terraform state. After a reset, run `make infra-apply` to reconcile state. `terraform -chdir=infrastructure/terraform destroy` removes the managed bucket only when empty; do this before `make clean` if you want to remove Terraform state too.

See [compatibility evidence](docs/compatibility.md) and [checklist](docs/checklist.md). Athena results are not evidence of AWS Athena compatibility or performance.
