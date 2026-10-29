# Project: Aster Data Platform

**Type:** Local-first AWS Data Engineering & Solutions Architecture Laboratory  
**Status:** Greenfield  
**Primary implementation agent:** Codex  
**Primary objective:** Build a verifiable, reproducible, AWS-compatible analytical data platform that can serve as a technical portfolio, learning laboratory, and reference architecture for client engagements.

## 1. Executive Summary

Aster is an open-source reference implementation of an operational data platform for a fictional telecom/SaaS business.

The platform ingests transactional and operational data from PostgreSQL, stores immutable raw datasets in an S3-compatible data lake, transforms data into analytical formats, and exposes business metrics through SQL.

The project uses MiniStack to emulate supported AWS services locally, Terraform/OpenTofu to provision infrastructure, Apache Airflow to orchestrate workloads, and DuckDB to execute analytical queries.

Aster must support controlled experiments involving ingestion failures, replay, schema changes, partition design, query optimization, and data correctness.

The system must remain explicitly identified as an AWS-compatible local proof of concept until real AWS deployment and verification have been completed.

## 2. Business Scenario

A fictional enterprise operates customer subscriptions and receives operational events from its applications.

Its reporting system currently queries transactional databases directly, causing resource contention and slow analytical queries.

The customer needs a separate analytics platform that:

- Consolidates data from transactional and event sources.
- Supports historical analysis without overloading operational databases.
- Produces consistent daily revenue, active customer, and service reliability metrics.
- Handles late or duplicate events.
- Supports reprocessing without duplicating business results.
- Can eventually be migrated to managed AWS services.
- Offers predictable operational costs and manageable complexity.

All business requirements are simulated. No real customer or production deployment is implied.

## 3. Project Goals

### G1. AWS Infrastructure Competence

Demonstrate familiarity with:

- AWS-compatible S3 object storage.
- IAM concepts and least-privilege design.
- AWS Glue data catalog concepts.
- AWS Athena analytical query interfaces.
- Terraform provisioning, state and dependency management.
- Cloud deployment and operational patterns.

### G2. Data Engineering Competence

Demonstrate:

- Source extraction and incremental ingestion.
- Data lake zoning.
- Partitioned Parquet storage.
- Transformation and data modeling.
- Data quality validation.
- Backfills and reprocessing.
- Orchestration and observability.
- Data pipeline testing.

### G3. Reliability Engineering

Validate:

- Idempotent ingestion.
- Safe checkpoint progression.
- Recovery after interrupted jobs.
- Duplicate-event handling.
- Late-arriving data.
- Failed transformation recovery.
- Repeatable infrastructure provisioning.

### G4. Solutions Engineering

Produce:

- A customer requirements document.
- At least three architecture decision records.
- An architecture diagram.
- A reproducible demonstration.
- A technical benchmark report.
- A realistic AWS migration strategy.
- A cost and operational trade-off assessment.

## 4. Non-Goals

The initial release must not attempt to:

- Reimplement AWS cloud services.
- Run production workloads.
- Build a generic enterprise data integration framework.
- Implement a custom distributed processing engine.
- Build a frontend dashboard.
- Support AWS, Azure and GCP simultaneously.
- Guarantee production-level AWS scalability through emulation.
- Introduce Kubernetes without a demonstrated need.
- Introduce Kafka, Spark, Iceberg, Redshift or Debezium in the initial vertical slice.

These technologies may be introduced as independently scoped extensions.

## 5. Technology Decisions

| Component | Technology | Responsibility |
|---|---|---|
| Local runtime | Docker Compose | Service lifecycle |
| AWS emulator | MiniStack Full | AWS-compatible local APIs |
| IaC | Terraform | Provision supported AWS resources |
| Source database | PostgreSQL | Transactional operational data |
| Ingestion | Python 3.12+ | Incremental extraction and object writing |
| Data format | Parquet / PyArrow | Columnar analytical storage |
| Orchestration | Apache Airflow | Scheduling, retries and backfills |
| Transformation | Python + DuckDB SQL | Staging and analytical models |
| Analytical engine | DuckDB | Local SQL execution and benchmarking |
| AWS query interface | MiniStack Athena | Optional verified integration |
| Quality testing | pytest + SQL assertions | Correctness and recovery verification |
| Dependency management | uv | Python dependencies |
| Linting | Ruff | Code quality |
| CI | GitHub Actions | Automated validation |

Use stable, compatible, pinned versions.

The agent must verify the actual MiniStack API coverage before implementing Glue/Athena-specific integration.

Do not emulate functionality by fabricating successful AWS API responses.

## 6. High-Level Architecture

```text
                  Source Layer
                ┌───────────────┐
                │ PostgreSQL    │
                │ customers     │
                │ subscriptions │
                │ payments      │
                │ events        │
                └───────┬───────┘
                        │
                  Incremental
                   Extraction
                        │
                        ▼
              ┌──────────────────┐
              │ S3 Raw / Bronze  │
              │ MiniStack        │
              │ JSONL / Parquet  │
              └────────┬─────────┘
                       │
                  Validation
                  Deduplication
                  Normalization
                       │
                       ▼
              ┌──────────────────┐
              │ S3 Silver        │
              │ Parquet          │
              └────────┬─────────┘
                       │
                 SQL Modeling
                       │
                       ▼
              ┌──────────────────┐
              │ S3 Gold          │
              │ Fact / Dimension │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ DuckDB / Athena  │
              │ Analytical SQL   │
              └──────────────────┘

    Airflow orchestrates pipeline execution.
    Terraform provisions emulated AWS resources.
    Tests validate data correctness and recovery.
```

### Execution Boundary

MiniStack runs in Docker and exposes AWS-compatible endpoints.

PostgreSQL, Airflow, transformation workers, and DuckDB are real local workloads.

Only supported cloud resources are emulated.

The application must use standard AWS client libraries where appropriate, with configuration controlling the endpoint.

## 7. Data Model

### Source Tables

**customers**

- customer_id: UUID, primary key
- name: string
- region: string
- created_at: timestamp with timezone
- updated_at: timestamp with timezone

**subscriptions**

- subscription_id: UUID, primary key
- customer_id: UUID, foreign key
- plan_name: string
- monthly_price: decimal
- status: string
- created_at: timestamp with timezone
- updated_at: timestamp with timezone

**payments**

- payment_id: UUID, primary key
- subscription_id: UUID, foreign key
- amount: decimal
- currency: string
- status: string
- paid_at: nullable timestamp with timezone
- created_at: timestamp with timezone
- updated_at: timestamp with timezone

**service_events**

- event_id: UUID, primary key
- customer_id: UUID, foreign key
- event_type: string
- event_time: timestamp with timezone
- ingested_at: timestamp with timezone
- duration_ms: nullable integer
- status_code: nullable integer
- payload: JSON

### Analytical Tables

**dim_customer**

- customer_key
- customer_id
- region
- created_date

**dim_subscription**

- subscription_key
- subscription_id
- plan_name
- monthly_price
- status

**fact_payments**

- payment_id
- customer_key
- subscription_key
- amount
- payment_date
- payment_status

**fact_service_events**

- event_id
- customer_key
- event_date
- event_type
- duration_ms
- status_code

### Required Analytical Queries

1. Daily successful payment revenue.
2. Daily active customers.
3. Revenue by customer region.
4. Revenue by subscription plan.
5. Service error rate by day and region.
6. p95 service event duration by day.
7. Daily event volume.
8. Seven-day revenue trend.

Define the semantics of every metric explicitly. In particular, document event-time versus ingestion-time attribution, timezone handling, payment status filtering, and unique-customer counting.

## 8. Functional Requirements

### FR-001: Infrastructure Provisioning

Terraform must provision the supported local AWS resources.

Required initial resources:

- Raw S3 bucket.
- Silver S3 bucket.
- Gold S3 bucket.
- Artifact/query results S3 bucket.

Implement an AWS provider endpoint override.

Resource naming must be deterministic and environment-specific.

Do not store actual credentials in source control.

A second Terraform plan with unchanged inputs should report no changes.

### FR-002: Source Initialization

Create SQL migrations for all four source tables.

A seed generator must create deterministic synthetic data using a configurable random seed.

Support dataset sizes:

- Small: 1,000 events.
- Medium: 100,000 events.
- Large: 1,000,000 events.

The large dataset is optional for initial CI.

Synthetic data should include valid relationships, multiple regions, multiple subscription plans, and realistic timestamp distributions.

### FR-003: Incremental Ingestion

Implement a batch extractor from PostgreSQL to the raw data lake.

Requirements:

- Configurable batch size.
- Composite cursor using `(updated_at, primary_key)` for mutable tables.
- An appropriate append-only cursor for events.
- Stable ordering.
- Durable checkpoint state.
- Atomic publication of completed batches.
- Retry support.
- Unique batch identifiers.
- No checkpoint advancement before committed output exists.

Define behavior for concurrent updates and cursor ties.

The first implementation may use application-managed checkpoints in PostgreSQL.

Do not introduce CDC until the batch ingestion path is correct.

### FR-004: Raw Data Storage

Persist raw data under deterministic prefixes.

Example:

```text
s3://aster-raw/
  customers/
    ingest_date=2026-10-29/
      batch=<batch-id>/
        data.parquet
        manifest.json
```

Each manifest must contain:

- Dataset name.
- Batch identifier.
- Source cursor range.
- Record count.
- Object paths.
- Checksums.
- Schema version.
- Creation timestamp.
- Completion status.

A batch must be considered committed only when its completion metadata and data objects are valid.

### FR-005: Silver Transformation

Normalize raw records into validated Parquet datasets.

Requirements:

- Enforce declared schemas.
- Reject or quarantine malformed records.
- Preserve source identifiers.
- Deduplicate records by business identity.
- Normalize timestamps to UTC.
- Preserve decimal precision.
- Record rejected-row counts.
- Support replay without duplicate results.

### FR-006: Gold Transformation

Build dimensional analytical tables.

Requirements:

- Deterministic transformations.
- Defined surrogate-key strategy.
- Referential integrity checks.
- Correct payment and event aggregation.
- Partitioned outputs where beneficial.
- Repeatable full refresh.
- Incremental processing where correctly supported.

Avoid assuming object stores support atomic in-place updates. Use versioned outputs and a committed-run manifest or equivalent publishing mechanism.

### FR-007: Analytical Queries

Provide SQL queries implementing the eight required metrics.

The initial execution engine must be DuckDB.

Add Athena API integration only if the relevant MiniStack functionality is verified and the queries return correct results.

All analytical queries must have fixture-based correctness tests.

### FR-008: Orchestration

Create an Airflow DAG:

```text
extract_sources
      ↓
validate_raw
      ↓
transform_silver
      ↓
validate_silver
      ↓
build_gold
      ↓
validate_gold
      ↓
publish_run
```

Requirements:

- Manual triggering.
- Configurable date window.
- Retry policies.
- Task logs.
- Failure propagation.
- Safe reruns.
- Backfill support.

Do not mark a pipeline successful until quality validation passes.

### FR-009: Quality Validation

Implement checks for:

- Primary-key uniqueness.
- Required fields.
- Foreign-key consistency.
- Nonnegative payment amounts.
- Valid subscription and payment statuses.
- Timestamp validity.
- Source-to-target row reconciliation.
- Metric correctness against known fixtures.

Validation failures must produce nonzero exit codes and actionable diagnostics.

### FR-010: Observability

Expose or log at least:

- Pipeline execution duration.
- Rows extracted.
- Rows transformed.
- Rows rejected.
- Bytes written.
- Last committed checkpoint.
- Current pipeline status.
- Failure category.
- Data freshness.

Use structured JSON logs with run IDs and dataset identifiers.

A dedicated monitoring stack is not required for v1.

### FR-011: Failure Recovery

Implement reproducible failure scenarios:

**Scenario A: Extraction interruption**

Terminate extraction after a batch is written but before checkpoint commitment.

Expected: Restart does not lose records or double-count them.

**Scenario B: Duplicate replay**

Replay a previously completed batch.

Expected: Silver and Gold results remain identical.

**Scenario C: Invalid source data**

Inject malformed records.

Expected: Invalid records are quarantined and reported without silent corruption.

**Scenario D: Transformation interruption**

Interrupt Gold generation before publication.

Expected: The previously committed Gold dataset remains queryable and unchanged.

**Scenario E: Backfill**

Reprocess a defined date range.

Expected: Results match a clean rebuild for the same data snapshot.

These tests must validate output data, not simply successful process exit codes.

## 9. Nonfunctional Requirements

### NFR-001: Reproducibility

The project must start from a fresh checkout using documented commands.

### NFR-002: Portability

AWS endpoints, region, credentials, and resource identifiers must be configurable.

Do not hardcode MiniStack URLs into business logic.

### NFR-003: Data Correctness

Exact reconciliation is required for deterministic fixture datasets.

No missing or duplicated business records are acceptable in passing correctness tests.

### NFR-004: Idempotency

Repeating a committed ingestion or transformation must not change the logical final result.

### NFR-005: Resource Efficiency

The default development profile must use small datasets and avoid unnecessary services.

Document minimum practical machine requirements after measurement.

### NFR-006: Security

Use synthetic data only.

No production credentials.

Do not print connection secrets.

Use least-privilege IAM policies in the production architecture even if local enforcement is incomplete.

### NFR-007: Maintainability

Business logic must be separate from AWS adapters, orchestration and infrastructure code.

Prefer straightforward modules over premature framework abstractions.

## 10. Repository Structure

```text
aster-data-platform/
├── AGENTS.md
├── README.md
├── Makefile
├── .env.example
├── .gitignore
├── .github/
│   └── workflows/
│       ├── lint.yml
│       └── integration.yml
├── infrastructure/
│   ├── compose.yaml
│   └── terraform/
│       ├── providers.tf
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── modules/
│           └── data-lake/
├── src/
│   └── aster/
│       ├── config/
│       ├── domain/
│       ├── ingestion/
│       ├── storage/
│       ├── transformation/
│       ├── quality/
│       └── cli/
├── orchestration/
│   └── dags/
├── sql/
│   ├── migrations/
│   ├── silver/
│   ├── gold/
│   └── analytics/
├── fixtures/
├── scripts/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── e2e/
│   └── failure/
├── benchmarks/
│   ├── results/
│   └── methodology.md
└── docs/
    ├── requirements.md
    ├── architecture.md
    ├── aws-migration.md
    ├── cost-model.md
    ├── demo-script.md
    ├── limitations.md
    └── adr/
```

The agent may refine names and package boundaries when justified, but must preserve clear separation between application code, IaC, tests and documentation.

## 11. Developer Experience

Expose the following commands through a Makefile or equivalent task runner.

```bash
make doctor
make up
make down
make infra-init
make infra-plan
make infra-apply
make seed
make ingest
make transform
make pipeline
make query
make test
make test-integration
make test-failure
make benchmark
make clean
```

Requirements:

- `doctor` verifies prerequisites and connectivity.
- `up` starts the local dependencies.
- `infra-apply` provisions supported AWS-compatible resources.
- `seed` generates deterministic source data.
- `pipeline` executes the complete ingestion and transformation path.
- `query` prints analytical results.
- `clean` clearly identifies destructive operations.

Commands must fail explicitly when dependencies or configuration are missing.

## 12. Implementation Milestones

### M0: Compatibility Spike

Objective: Confirm the actual toolchain works before scaffolding the complete platform.

Tasks:

1. Start MiniStack Full.
2. Verify its health endpoint.
3. Initialize Terraform.
4. Provision one S3 bucket.
5. Upload and retrieve a test object.
6. Run a DuckDB query against a local Parquet fixture.
7. Test Athena API integration separately.
8. Record supported operations and limitations.

**Definition of Done:** A passing compatibility test and an evidence-based implementation decision.

### M1: Infrastructure Foundation

Implement:

- Docker Compose.
- PostgreSQL.
- MiniStack.
- Terraform buckets.
- Configuration.
- Makefile.
- Health checks.
- Initial documentation.

**Definition of Done:** Fresh checkout, provisioning and clean teardown work.

### M2: Data Ingestion

Implement:

- SQL migrations.
- Seed generator.
- Incremental extractor.
- Parquet raw storage.
- Checkpoint logic.
- Batch manifests.
- Integration tests.

**Definition of Done:** Ingestion can be replayed without data loss or duplicate logical output.

### M3: Analytical Modeling

Implement:

- Silver transformation.
- Gold dimensional models.
- Analytical SQL.
- Quality tests.
- Data reconciliation.

**Definition of Done:** All eight analytical queries pass fixture correctness checks.

### M4: Orchestration

Implement:

- Airflow.
- Pipeline DAG.
- Retries.
- Backfills.
- Structured logs.
- Pipeline status and data freshness.

**Definition of Done:** A complete pipeline can be run and rerun through Airflow.

### M5: Reliability Laboratory

Implement all five required failure scenarios.

**Definition of Done:** Automated tests prove recovery behavior and resulting data correctness.

### M6: Portfolio Packaging

Implement:

- Architecture diagram.
- Technical README.
- Three or more ADRs.
- Benchmark methodology and measured results.
- Customer-facing case study.
- AWS migration plan.
- Cost model.
- Demonstration script.

**Definition of Done:** Another engineer can reproduce the demo without undocumented manual steps.

## 13. Test Strategy

### Unit Tests

Test:

- Cursor comparisons.
- Checkpoint transitions.
- Schema validation.
- Metric logic.
- Data deduplication.
- Manifest generation.
- Configuration parsing.

### Integration Tests

Run against real local PostgreSQL and MiniStack.

Verify:

- Bucket provisioning.
- Object read/write.
- Extraction.
- Checkpoint persistence.
- Parquet output.
- SQL transformations.

### End-to-End Tests

Execute:

```text
Fresh infrastructure
   → Seed database
   → Run complete pipeline
   → Query analytical models
   → Compare expected results
```

### Failure Tests

Inject process termination, duplicate inputs, invalid records and partial output generation.

Every failure test must assert committed state and final data correctness.

## 14. Benchmarking

Measure:

- Ingestion rows/second.
- Extraction duration.
- Transformation duration.
- Parquet storage size.
- Compression ratio.
- Query execution time.
- Partition-pruning impact.
- Recovery time after interruption.

Run benchmarks with documented:

- Dataset sizes.
- Hardware specifications.
- Configuration.
- Number of repetitions.
- Measurement methodology.

Never invent benchmark results.

Do not present MiniStack/DuckDB benchmarks as AWS Athena or Redshift performance.

## 15. AWS Migration Strategy

Keep the local development environment separate from real AWS configuration.

Required future migration steps:

1. Create an isolated AWS account or sandbox environment.
2. Configure secure AWS credentials.
3. Introduce real IAM policies.
4. Implement VPC/networking where required.
5. Replace the MiniStack S3 endpoint with AWS S3.
6. Deploy a representative Glue/Athena integration.
7. Verify AWS-specific catalog behavior.
8. Configure CloudWatch logs and cost monitoring.
9. Execute a bounded workload.
10. Record actual costs and operational differences.
11. Destroy temporary infrastructure safely.

Real AWS deployment is an extension, not a dependency of the local MVP.

No real AWS resources should be created without explicit approval.

## 16. Solutions Engineering Deliverables

### Customer Discovery Document

Describe:

- Customer business context.
- Data sources.
- Reporting requirements.
- Volume and freshness constraints.
- Recovery expectations.
- Security requirements.
- Cost constraints.
- Existing technical capabilities.

### Architecture Alternatives

Evaluate:

**Option A:** S3 + Athena + Glue.

**Option B:** S3 + Spark + Iceberg.

**Option C:** PostgreSQL + ClickHouse.

Compare:

- Initial implementation effort.
- Operating complexity.
- Scaling characteristics.
- Query latency.
- Data freshness.
- Recovery model.
- Security/governance.
- Cost drivers.

Do not invent performance or pricing outcomes. Use explicit assumptions where direct evidence is unavailable.

### Architecture Decision Records

Minimum:

- ADR-001: Why local AWS emulation?
- ADR-002: Why Parquet and a zoned data lake?
- ADR-003: Why incremental batch ingestion first?
- ADR-004: Why the selected analytical engine?
- ADR-005: How committed datasets are published safely?

### Customer Demonstration

A short demonstration must show:

1. Infrastructure provisioning.
2. Source data ingestion.
3. Analytical query results.
4. A deliberate pipeline failure.
5. Successful recovery.
6. Architecture and cost trade-offs.

## 17. Acceptance Criteria

A milestone is complete only when:

- Its implementation works.
- Tests pass.
- Relevant documentation exists.
- No real AWS credentials are required.
- It can run on a clean local environment.
- Unsupported emulator functionality is not hidden behind mock-success behavior.

Project v1 is accepted when:

- Terraform provisions the required local resources.
- PostgreSQL source data is ingested incrementally.
- Silver and Gold Parquet outputs are generated.
- Eight analytical metrics are correct.
- Airflow executes the complete pipeline.
- Five failure scenarios pass.
- The repository is reproducible.
- The portfolio case study accurately identifies local emulation.

## 18. Rules for the Codex Agent

1. Work incrementally, following the defined milestones.
2. Begin with M0, not the full platform.
3. Favor the smallest working vertical slice.
4. Verify dependencies and external API behavior before implementation.
5. Never invent AWS emulator features.
6. Never silently substitute mocks for actual integrations.
7. Prefer simple, testable code over elaborate abstractions.
8. Maintain clear domain and infrastructure boundaries.
9. Keep tests alongside implemented functionality.
10. Do not add services without a documented use case.
11. Do not introduce unnecessary agent frameworks or AI integrations.
12. Do not claim performance without measurements.
13. Never deploy to real AWS without explicit authorization.
14. Maintain a running implementation checklist.
15. Document blockers and unsupported features.
16. At the end of each milestone, report changed files, commands run, test results, and remaining work.

## 19. Immediate Task

**Scaffold and implement M0 only.**

Create the initial repository structure, local development configuration, Terraform provider configuration, Docker Compose environment, smoke tests, basic documentation, and Codex instructions.

Verify MiniStack startup, S3 provisioning, S3 read/write and DuckDB execution.

Explore Athena compatibility but do not let it block the local S3 + DuckDB pipeline.

Do not implement Airflow, source extraction, warehouse transformations or additional services until M0 is complete.

The expected result is an executable foundation, not placeholder files.

