# Phone Compliance System

A data pipeline that determines whether SMS outreach is permitted for a given phone number on a customer account. Processes ENRLMT (enrollment), NON-FIN-BDL (account snapshots), and SUPPLFWD (change history) files through a Bronze → Silver → Gold architecture using PySpark.

## Table of Contents

- [Architecture](#architecture)
- [Pipeline Overview](#pipeline-overview)
- [Quick Start](#quick-start)
- [Local Development](#local-development)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
  - [Running the Pipeline](#running-the-pipeline)
  - [Test Data](#test-data)
  - [Tests](#tests)
- [Cloud Deployment (Databricks on AWS)](#cloud-deployment-databricks-on-aws)
  - [Cloud Prerequisites](#cloud-prerequisites)
  - [AWS IAM Setup](#aws-iam-setup)
  - [Databricks Setup](#databricks-setup)
  - [Permissions Created by Terraform](#permissions-created-by-terraform)
  - [Deployment Steps](#deployment-steps)
  - [SQL Validation](#sql-validation)
- [Design Decisions](#design-decisions)
  - [Pipeline Logic](#pipeline-logic)
  - [Edge Cases](#edge-cases)
  - [Deployment Tradeoffs](#deployment-tradeoffs)
  - [Bonus Challenges](#bonus-challenges)
- [Gold Schema](#gold-schema)
- [Project Structure](#project-structure)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          AWS Account                                │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐                               │
│  │ S3: Raw Data │    │ S3: Delta    │                               │
│  │ ENROLLMENT/  │    │ bronze/      │                               │
│  │ MAINTENANCE/ │    │ silver/      │                               │
│  │ (csv.gz)     │    │ gold/        │                               │
│  └──────┬───────┘    └──────▲───────┘                               │
│         │                   │                                       │
│         │            ┌──────┴───────┐                               │
│         └───────────►│  Databricks  │  Pipeline code on DBFS        │
│                      │  Single-node │  S3A creds via spark_conf     │
│                      │  Job         │  Delta Change Data Feed on    │
│                      │              │                               │
│                      │ Bronze ──►   │                               │
│                      │ Silver ──►   │                               │
│                      │ Gold   ──►   │                               │
│                      └──────────────┘                               │
└─────────────────────────────────────────────────────────────────────┘
```

### Table Dependency Graph

```
  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
  │  S3 Raw Files    │     │  S3 Raw Files    │     │  S3 Raw Files    │
  │  NON-FIN-BDL     │     │  SUPPLFWD        │     │  ENRLMT          │
  │  (monthly snap)  │     │  (weekly changes)│     │  (enrollments)   │
  └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
           │                        │                         │
           ▼                        ▼                         ▼
  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
  │  Bronze: nonfin  │     │ Bronze: supplfwd │     │ Bronze: enrlmt   │
  │  (Delta)         │     │ (Delta)          │     │ (Delta)          │
  └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
           │                        │                         │
           └────────────┬───────────┘                         │
                        │                                     │
                        ▼                                     │
               ┌──────────────────┐                           │
               │  Silver:         │◄──────────────────────────┘
               │  phone_state     │  enrollment-only phones
               │  (Delta)         │  via left_anti join
               │                  │
               │  1 row per       │
               │  (account,phone) │
               └────────┬─────────┘
                        │
                        ▼
               ┌──────────────────┐
               │  Gold:           │
               │  phone_consent   │
               │  (Delta, MERGE)  │
               │                  │
               │  +1XXXXXXXXXX    │
               │  can_send_sms()  │
               └──────────────────┘
```

---

## Pipeline Overview

| Layer | Input | Output | Key Logic |
|-------|-------|--------|-----------|
| **Bronze** | Raw gzipped CSVs from S3 | 3 Delta tables (`nonfin`, `supplfwd`, `enrollment`) | Parse pipe-delimited files, extract `_file_date` from filenames |
| **Silver** | 3 Bronze Delta tables | 1 Delta table (`phone_state`) | Dedup, hard/soft delete detection, NON-FIN vs SUPPLFWD merge, enrollment fallback → 1 row per `(account, phone)` |
| **Gold** | Silver `phone_state` | 1 Delta table (`phone_consent`) | Normalize phones to `+1XXXXXXXXXX`, derive `has_sms_consent`, expose `can_send_sms()` compliance API |

### `can_send_sms` Rules

Returns **True** only when all conditions are met:
1. The `(account_number, phone_number)` row exists in `phone_consent`
2. `phone_source = 'CLIENT'`
3. `is_deleted = False`
4. `has_sms_consent` is `True` or `NULL` (implicit consent allowed)

Returns **False** otherwise (including when no matching row is found).

---

## Quick Start

```bash
# Local
uv sync --extra dev
uv run python generate_sample_data.py
uv run python -m pipeline.run_gold ./data_sample
uv run pytest tests/ -v

# Cloud (after AWS/Databricks setup — see Cloud Deployment below)
make init && make deploy
make generate-data && make push-data
make push-code
make run-pipeline
```

---

## Local Development

### Prerequisites

- Python 3.12+
- Java 17 (required by PySpark 3.5 — Java 18+ is not compatible)
- [uv](https://docs.astral.sh/uv/)

#### Java 17 Setup

PySpark 3.5 is incompatible with Java 18+ due to a removed `Subject.getSubject` API. Install Java 17 if you don't have it:

```bash
# macOS (via Homebrew)
brew install --cask corretto17

# Verify
/usr/libexec/java_home -V
```

The pipeline auto-detects Corretto 17 at its default install path. If your Java 17 is elsewhere, set `JAVA_HOME` before running:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### Setup

```bash
uv sync --extra dev
```

### Running the Pipeline

```bash
# Bronze — verify PySpark can read all three file types
uv run python -m pipeline.run_bronze ./data_sample

# Silver — reconcile into one row per (account, phone)
uv run python -m pipeline.run_silver ./data_sample

# Gold — full pipeline with compliance checks
uv run python -m pipeline.run_gold ./data_sample
```

Replace `./data_sample` with `./data` to run against the full 1GB dataset.

### Test Data

**Full dataset** (~1GB compressed, ~5GB uncompressed):
```bash
uv run python generate_bank_data.py --output-dir ./data --target-size-gb 1.0
```

**Sample dataset** (~10MB, ~500 accounts):
```bash
uv run python generate_sample_data.py
```
Creates `data_sample/` (gzipped for PySpark) and `data_preview/` (plain CSV for manual inspection). Both are gitignored.

### Tests

```bash
# Unit + integration (default, ~35s)
make test
# or: uv run pytest tests/ -v

# E2E with freshly generated data (~60s)
uv run pytest tests/ -v -m "e2e"

# E2E against existing data_sample/
uv run pytest tests/ -v -m "e2e" --e2e-data=sample

# Everything
make test-all
# or: uv run pytest tests/ -v -m ""
```

| Tier | File | Tests | Default | Description |
|------|------|------:|---------|-------------|
| Unit | `test_bronze.py` | 14 | Yes | Schema, columns, types, file-date parsing |
| Unit | `test_silver.py` | 32 | Yes | Dedup, deletes, merging, enrollment, tie-breaking |
| Unit | `test_gold.py` | 25 | Yes | Normalization, consent, `can_send_sms` compliance |
| Integration | `test_integration.py` | 8 | Yes | Cross-layer contracts, idempotency, full pipeline |
| E2E | `test_e2e.py` | 11 | No | Data quality, consistency, business logic at scale |
| **Total** | | **90** | **79** | |

Unit and integration tests use self-contained fixtures (tiny gzipped CSVs written to temp directories) and don't require generated data. E2E tests are excluded by default (`addopts = "-m 'not e2e'"` in `pyproject.toml`).

---

## Cloud Deployment (Databricks on AWS)

### Cloud Prerequisites

- AWS CLI configured with credentials
- Terraform >= 1.5
- Databricks CLI configured (for DBFS uploads via `make push-code`). Must be on your `PATH` for `make` targets — if installed via `pip3`, you may need:
  ```bash
  export PATH="/Library/Frameworks/Python.framework/Versions/3.13/bin:$PATH"  # macOS pip3 default
  ```
- An existing Databricks workspace on AWS

### AWS IAM Setup

Create a dedicated IAM user for deployment (do not use root):

1. **Create IAM user** — Go to IAM → Users → Create user (e.g., `phone-compliance-admin`)
2. **Attach policy** — `AdministratorAccess` (needs S3, ECR, and IAM permissions for Terraform)
3. **Create access key** — Security credentials → Create access key → CLI
4. **Configure CLI** — Run `aws configure` with the access key, secret key, and region

### Databricks Setup

1. **Create workspace** — Sign up at [databricks.com](https://www.databricks.com/) and create an AWS-backed workspace
2. **Generate token** — In the Databricks UI: Profile → Settings → Developer → Access tokens → Generate new token
3. **Find cross-account role** — When Databricks creates a workspace, it provisions a cross-account IAM role in your AWS account named `databricks-compute-role-XXXXX`. Find it in IAM → Roles. Terraform attaches an S3 access policy to this role so job clusters can read/write your data buckets.
4. **Configure CLI** — Run `databricks configure --token` with your workspace URL and token

### Permissions Created by Terraform

| Resource | Purpose |
|----------|---------|
| **S3 buckets** (x2) | Raw data bucket + Delta Lake bucket (versioning enabled on Delta) |
| **S3 access policy** on `databricks-compute-role-*` | Grants the Databricks cross-account role `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on both buckets |
| **ECR repository** | Container registry (provisioned for Docker-based deployment — see [Container Services note](#container-services-docker) below) |

The Databricks job cluster receives AWS credentials via `spark.hadoop.fs.s3a.*` spark conf so that Spark can read/write S3 directly. These are the same credentials used for `aws configure`.

> **Note**: On Databricks free-trial workspaces, instance profiles are blocked by a permissions boundary on the cross-account role (`iam:PassRole` is denied). The S3A credentials approach works around this. For production deployments, replace with instance profiles or Unity Catalog storage credentials.

### Deployment Steps

```bash
# 1. Configure infrastructure
cp infra/terraform.tfvars.example infra/terraform.tfvars
# Edit terraform.tfvars with your values:
#   - s3_data_bucket_name, s3_delta_bucket_name (globally unique)
#   - databricks_host (workspace URL)
#   - databricks_token (personal access token)
#   - databricks_cross_account_role_name (e.g., "databricks-compute-role-7474649069652795")
#   - aws_access_key_id, aws_secret_access_key (for cluster S3 access)

# 2. Deploy infrastructure (S3, ECR, IAM policy, Databricks job)
make init && make deploy

# 3. Generate and upload data
make generate-data
make push-data

# 4. Upload pipeline code to DBFS
make push-code

# 5. Run the pipeline
make run-pipeline
```

### SQL Validation

After the pipeline completes, run the SQL validation queries in `tests/sql_tests/` against the Delta tables on Databricks (via a notebook or SQL editor). Each file covers one layer and validates all edge cases:

```
tests/sql_tests/
  validate_bronze.sql    # 9 queries: ingestion completeness, schema, file-date parsing
  validate_silver.sql    # 18 queries: dedup, deletes, merges, all 6 edge cases
  validate_gold.sql      # 18 queries: normalization, consent, can_send_sms compliance
```

---

## Design Decisions

### Pipeline Logic

- **Hard delete detection**: Single `groupBy` + `max(_file_date)` per phone vs global max snapshot date. Avoids expensive pairwise snapshot comparisons.
- **Merge strategy**: Union NON-FIN and SUPPLFWD into a common schema, then `row_number()` window to pick the most recent record. More efficient than a full outer join.
- **Enrollment fallback**: Enrollment-only phones (never in NON-FIN or SUPPLFWD) are included via `left_anti` join — they only fill gaps, never override fresher data.
- **Account number**: Derived in Silver by stripping the trailing "P" from `cnsmr_idntfr_lgcy_txt`.
- **Implicit consent**: `can_send_sms` treats NULL consent as permissive (implicit consent allowed) per business rules. Only an explicit `N` (consent withdrawn) blocks SMS outreach.
- **Account-phone independence**: Each `(account_number, phone_number)` pair is evaluated independently. The same phone number on different accounts can have different deletion and consent states.

### Edge Cases

| Edge Case | Approach |
|-----------|----------|
| Same phone on multiple accounts | Each `(account, phone)` pair tracked independently through all layers |
| Phone re-added after deletion | If winning NON-FIN record is newer than the soft-delete date, the soft delete is overridden |
| Consent changes over time | Latest record (by event date) determines current consent status |
| Missing consent flag | Treated as implicit consent (NULL) — `can_send_sms` returns True |
| SUPPLFWD vs NON-FIN conflicts | Most recent event date wins; SUPPLFWD breaks ties |
| Duplicate records in same file | Deduplicated by `phone_id` (NON-FIN) or `(legacy_id, phone, record_date)` (SUPPLFWD) |
| Enrollment-only phones | Included in Silver/Gold if never seen in NON-FIN or SUPPLFWD |

### Deployment Tradeoffs

#### Current approach

Infrastructure and pipeline code are deployed together through a single Terraform configuration + DBFS upload:

- **Terraform** manages all resources: AWS (S3 buckets, ECR, IAM policy) and Databricks (job definition, cluster config)
- **`make push-code`** uploads pipeline Python files to DBFS via the Databricks CLI
- Cloud entrypoints use `sys.path.insert(0, "/dbfs/phone-compliance")` so that `from pipeline.bronze import ...` resolves against the DBFS-uploaded code

This works reliably on free-trial workspaces but has drawbacks: DBFS file uploads are not versioned, there is no dependency management on the cluster (the pipeline relies on packages pre-installed in the Databricks runtime), and Terraform manages both AWS infrastructure and Databricks job config in a single state file.

#### Recommended production approach

Split responsibilities between two tools:

- **Terraform** for AWS infrastructure only (S3 buckets, IAM policies, ECR) — resources that change infrequently
- **Databricks Asset Bundles (DAB)** for pipeline code and Databricks job definitions — resources that change with every code push

Asset Bundles handle code packaging, dependency installation, workspace file sync, and job configuration in a single `databricks.yml`. They version pipeline code alongside job definitions, support CI/CD natively, and eliminate the need for DBFS path hacks or manual `databricks fs cp` uploads. This is the Databricks-recommended pattern for production workflows.

#### Why not Delta Live Tables (DLT)?

Delta Live Tables would be a natural fit for a medallion pipeline — it manages table dependencies declaratively, handles retries, and provides built-in data quality expectations. However, DLT was intentionally not used here:

- **Testing parity**: The pipeline is designed to run identically on local PySpark and on Databricks. All 90 tests run locally against the same `bronze.py`, `silver.py`, and `gold.py` modules that execute in the cloud. DLT's `@dlt.table` decorator API is Databricks-only and cannot be tested with a local SparkSession, which would break this parity.
- **Portability**: Standard PySpark code can be developed and debugged locally without a Databricks workspace, which reduces iteration time significantly. DLT requires a running Databricks cluster for any execution, even during development.
- **Time constraints**: Rewriting the pipeline to use DLT's declarative API would require restructuring all three layers and rewriting the test suite against DLT's testing framework (`dlt.test`). The standard PySpark approach allowed faster development while maintaining full test coverage.

For a production system where the pipeline will only run on Databricks, DLT is worth adopting — particularly for its built-in expectations (replacing `data_quality.py`) and automatic dependency resolution between tables.

#### Why ECR / Docker is not used <a id="container-services-docker"></a>

A Docker-based deployment (custom image from ECR via Databricks Container Services) was attempted but encountered multiple blockers on the free-trial workspace:

1. **Container Services disabled by default** — The `enableDcs` workspace setting is not exposed in the admin UI. It can be toggled via the REST API (`PATCH /api/2.0/workspace-conf`), but the feature remained unreliable on the free trial.
2. **ECR authentication blocked** — The cross-account role's permissions boundary blocks `ecr:*` actions, so the cluster cannot natively pull from a private ECR repository. A workaround using a temporary ECR authorization token via `docker_image.basic_auth` in Terraform resolved the auth error, but the token expires every 12 hours.
3. **Container creation failure** — After resolving authentication, the cluster failed with `DOCKER_CONTAINER_CREATION_EXCEPTION`. Databricks Container Services requires that custom images do not set an `ENTRYPOINT` (Databricks manages the entrypoint to initialize Spark). After fixing the Dockerfile, the container still failed to create on the free-trial environment.

A `Dockerfile` and ECR repository are included in this project. For production workspaces with Container Services enabled and proper instance profiles, the `docker_image` block can be added back to `databricks.tf` and the `Makefile` targets (`make build`, `make push-image`) are ready to use.

### Bonus Challenges

#### Incremental Processing

**Approach**: The Gold cloud entrypoint uses Delta Lake `MERGE` (upsert by `account_number + phone_number`) instead of full overwrite. Bronze and Silver use overwrite since they reprocess from source.

**Pros**:
- Gold MERGE is the highest-value target — downstream consumers query this table, and upserting avoids rewriting millions of rows when only a fraction change
- Delta Lake MERGE is ACID-compliant — concurrent readers always see a consistent snapshot, even mid-write
- Natural fit for daily runs: new source files flow through Bronze/Silver, and only changed `(account, phone)` pairs are touched in Gold

**Cons**:
- Bronze and Silver are still full-reprocess — Silver must consider all NON-FIN snapshots to detect hard deletes, so making it truly incremental would require tracking processed snapshots and recalculating only affected accounts (significant complexity)
- MERGE performance degrades as the Gold table grows into the billions; at that scale, partitioning by `account_number` prefix or Z-ordering would be needed
- An alternative — Auto Loader (Structured Streaming on S3) — would handle incremental file discovery natively and is worth considering since source files arrive daily. However, Silver's hard-delete detection requires comparing against _all_ NON-FIN snapshots (not just the new one), which limits the benefit of streaming ingestion. Auto Loader would reduce Bronze latency but the Silver full-reconciliation bottleneck remains

#### Data Quality Monitoring

**Approach**: `pipeline/data_quality.py` runs checks after each layer — null counts on key columns, uniqueness validation, format checks (phone number patterns, account number length), and row count sanity. Results are logged; failures halt the job.

**Pros**:
- Zero external dependencies — checks are pure PySpark, no additional frameworks to install or configure
- Self-contained pipeline: quality gates ship with the same code as the transforms, so they stay in sync as schemas evolve
- Fail-fast behavior: a failing check halts the job before downstream layers process bad data

**Cons**:
- No historical dashboard — check results are logged to stdout, not persisted to a metrics table. Trend detection (e.g., "null rate spiking over the past week") requires parsing logs
- Checks run after writes, not before — bad data can land in Delta before being flagged. Gating writes on checks would add latency and complicate retry logic
- A dedicated framework (Great Expectations, Deequ, or Databricks built-in expectations) would provide richer features out of the box: data profiling, anomaly detection, Slack/PagerDuty alerting. Worth adopting if this pipeline grows to support SLAs

#### Audit Trail

**Approach**: Two complementary mechanisms provide audit coverage:

1. **File-level lineage (`_source_file`)**: Every row in Bronze, Silver, and Gold carries a `_source_file` column containing the full path of the client file that drove its current state. This is captured at ingestion via Spark's `input_file_name()` and propagated through all layers — when Silver merges NON-FIN, SUPPLFWD, and ENRLMT records, the winning record's `_source_file` is preserved. This directly answers: "this Gold row looks like this because of `MRS_PLACE_20260201.txt`".

2. **Delta Change Data Feed (CDF)**: Enabled globally via Terraform `spark_conf`. Records row-level changes (inserts, updates, deletes) for every Delta table automatically. Supports `SELECT * FROM table_changes('table', start_version)` and time travel queries (`SELECT * FROM table VERSION AS OF 5`).

**Pros**:
- Every Gold row is traceable back to the specific client file that produced it — no re-running the pipeline or querying Bronze manually to debug
- CDF is effectively free — one Terraform config flag, minimal storage overhead
- Combined, the two mechanisms answer both "what changed and when" (CDF) and "which file caused it" (`_source_file`)
- Time travel enables point-in-time auditing (e.g., "what was this account's consent status on March 1st?") and easy rollback of bad writes

**Cons**:
- `_source_file` tracks lineage per row, not per column — if a Gold row's `phone_source` came from SUPPLFWD but `consent_flag` was originally set in NON-FIN, only the winning record's file is tracked. Per-column lineage (e.g., a snapshot table mapping `(table.column, row_id) → source_file`) would be richer but significantly more complex to implement and maintain
- Delta table history is bounded by the retention period (default 30 days for time travel, configurable). Long-term audit requirements need an explicit archival strategy (e.g., archiving CDF output to an append-only table before `VACUUM` runs)
- `_source_file` captures the file path at read time — if source files are moved or renamed in S3 after processing, the path becomes stale

#### Real-time API

**Approach**: The Gold Delta table is queryable via Databricks SQL Warehouse, meeting the <5s query requirement. A benchmark script (`tests/benchmark_can_send_sms.py`) validates this by running the `can_send_sms` SQL query against real data.

For sub-100ms lookups, the Gold table can be cached in an in-memory serving layer:

| Option | Latency | Tradeoff |
|--------|---------|----------|
| **Redis / ElastiCache** | <1ms | Load Gold table as `(account_number, phone_number) → can_send_sms` hash map. Invalidate on pipeline completion via a post-Gold webhook or Delta CDF consumer. Simple, fast, but cache staleness is possible between pipeline runs. |
| **DynamoDB** | 1-5ms | Same key-value pattern, but durable and serverless. Sync via Lambda consuming Delta CDF. More infrastructure, but no cache invalidation to manage — writes are idempotent upserts. |
| **Application-level cache** | <1ms | Load the full Gold table into a Lambda/container's memory at startup (~500K rows fits in <100MB). Refresh on a schedule or trigger. Simplest option for small-to-medium datasets, but doesn't scale past a few million rows. |

All three options use the same access pattern: the Gold `can_send_sms` result is a pure function of `(account_number, phone_number)`, making it ideal for key-value caching. The Gold Delta table remains the source of truth — the cache is a read-through optimization.

**Pros**:
- SQL Warehouse warm queries return in <2s with zero additional infrastructure, sufficient for batch compliance checks and internal dashboards
- Natural upgrade path: any caching layer can be added without changing the pipeline — Gold Delta remains the source of truth
- The `can_send_sms` access pattern (single key-value lookup, read-heavy, infrequent writes) is ideally suited for caching

**Cons**:
- SQL Warehouse cold-start latency is 5-10s (serverless warehouse spin-up), which is unacceptable for customer-facing API calls — a caching layer is required for production use
- All caching options introduce consistency lag between pipeline completion and cache refresh. For this use case (daily batch pipeline), staleness of minutes is acceptable
- DynamoDB/Redis add AWS infrastructure costs and operational overhead (monitoring, capacity planning). The application-level cache avoids this but requires careful memory management

---

## Gold Schema

### `phone_consent` table

| Column | Type | Description |
|--------|------|-------------|
| `account_number` | string | 16-digit account identifier |
| `phone_number` | string | Normalized to `+1XXXXXXXXXX` format |
| `phone_source` | string | `CLIENT`, `CONSUMER`, `THIRD_PARTY`, or `OTHER` |
| `has_sms_consent` | boolean (nullable) | `True`, `False`, or `NULL` (implicit consent) |
| `sms_consent_updated_at` | date | When consent was last updated |
| `is_deleted` | boolean | Whether the phone has been deleted |
| `delete_type` | string | `hard_delete`, `soft_delete`, or `NULL` |
| `deleted_at` | date | When the deletion occurred |

---

## Project Structure

```
pipeline/
  spark_session.py         # SparkSession factory (auto-detects local vs Databricks)
  bronze.py                # Bronze layer: read raw files into DataFrames
  silver.py                # Silver layer: reconcile into one row per (account, phone)
  gold.py                  # Gold layer: phone_consent table and can_send_sms API
  delta_io.py              # Delta Lake read/write/merge utilities (cloud only)
  data_quality.py          # Quality checks for each layer (cloud only)
  run_bronze.py            # Local Bronze runner
  run_silver.py            # Local Silver runner with summary stats
  run_gold.py              # Local Gold runner with compliance checks
  run_bronze_cloud.py      # Cloud entrypoint: S3 CSVs → Bronze Delta
  run_silver_cloud.py      # Cloud entrypoint: Bronze Delta → Silver Delta
  run_gold_cloud.py        # Cloud entrypoint: Silver Delta → Gold Delta (merge)
tests/
  conftest.py              # Shared fixtures, SparkSession, E2E data options
  test_bronze.py           # Bronze schema, columns, types, file-date parsing (14)
  test_silver.py           # Silver dedup, deletes, merging, tie-breaking (32)
  test_gold.py             # Gold normalization, consent, can_send_sms (25)
  test_integration.py      # Cross-layer contracts, idempotency, full pipeline (8)
  test_e2e.py              # Data quality and business logic at scale (11)
  sql_tests/
    validate_bronze.sql    # Bronze validation: ingestion, schema, file dates (9)
    validate_silver.sql    # Silver validation: dedup, deletes, merges, edge cases (18)
    validate_gold.sql      # Gold validation: normalization, consent, compliance (18)
infra/
  providers.tf             # AWS + Databricks providers
  variables.tf             # S3 buckets, Databricks config, AWS credentials, cluster settings
  s3.tf                    # Raw data + Delta Lake buckets (versioning on Delta)
  ecr.tf                   # ECR repository
  iam.tf                   # S3 access policy on Databricks cross-account role
  databricks.tf            # Single-node multi-task job: Bronze → Silver → Gold with S3A credentials
  outputs.tf               # Bucket names, ECR URL, job ID
  terraform.tfvars.example # Example configuration
Makefile                   # push-data, build, push-image, deploy, run-pipeline, test
Dockerfile                 # Databricks Container Services image (not used on free trial — see above)
generate_bank_data.py      # Full dataset generator (configurable size/seed)
generate_sample_data.py    # Sample data + preview generator for local dev
```
