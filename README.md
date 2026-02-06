# Phone Compliance System

A data pipeline that determines whether SMS outreach is permitted for a given phone number on a customer account. Processes ENRLMT (enrollment), NON-FIN-BDL (account snapshots), and SUPPLFWD (change history) files through a Bronze → Silver → Gold architecture using PySpark.

## Prerequisites

- Python 3.12+
- Java 17 (required by PySpark 3.5 — Java 18+ is not compatible)
- [uv](https://docs.astral.sh/uv/)

### Java 17 Setup

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

## Setup

```bash
uv sync --extra dev
```

## Generate Test Data

```bash
uv run python generate_bank_data.py --output-dir ./data --target-size-gb 1.0
```

This produces ~410 gzipped pipe-delimited CSV files (~1GB compressed) across `data/ENROLLMENT/` and `data/MAINTENANCE/`.

### Sample Data

Generate a small dataset for local development and manual inspection:

```bash
uv run python generate_sample_data.py
```

This creates two directories (both gitignored):
- `data_sample/` — small gzipped dataset for PySpark (~500 accounts)
- `data_preview/` — same files extracted as plain CSV for manual inspection

## Running

### Bronze Layer

Verify PySpark can read all three file types:

```bash
# Against sample data
uv run python -m pipeline.run_bronze ./data_sample

# Against full dataset
uv run python -m pipeline.run_bronze ./data
```

### Silver Layer

Run the full Bronze → Silver pipeline and display summary stats (delete types, consent breakdown, phone sources, winning record sources):

```bash
# Against sample data
uv run python -m pipeline.run_silver ./data_sample

# Against full dataset
uv run python -m pipeline.run_silver ./data
```

### Gold Layer

Run the full Bronze → Silver → Gold pipeline, display the `phone_consent` table, and run sample `can_send_sms` compliance checks:

```bash
# Against sample data
uv run python -m pipeline.run_gold ./data_sample

# Against full dataset
uv run python -m pipeline.run_gold ./data
```

### Tests

```bash
# Unit + integration (default, ~35s)
uv run pytest tests/ -v

# E2E with freshly generated data (~60s, generates a small dataset automatically)
uv run pytest tests/ -v -m "e2e"

# E2E against existing data_sample/ (quick spot-check, pairs with data_preview/)
uv run pytest tests/ -v -m "e2e" --e2e-data=sample

# Everything
uv run pytest tests/ -v -m ""
```

The test suite has three tiers:

| Tier | File | Tests | Default | Description |
|------|------|------:|---------|-------------|
| Unit | `test_bronze.py` | 14 | Yes | Schema, columns, types, file-date parsing |
| Unit | `test_silver.py` | 32 | Yes | Dedup, deletes, merging, enrollment, tie-breaking |
| Unit | `test_gold.py` | 25 | Yes | Normalization, consent, `can_send_sms` compliance |
| Integration | `test_integration.py` | 8 | Yes | Cross-layer contracts, idempotency, full pipeline |
| E2E | `test_e2e.py` | 11 | No | Data quality, consistency, business logic at scale |
| **Total** | | **90** | **79** | |

Unit and integration tests use self-contained fixtures (tiny gzipped CSVs written to temp directories) and don't require generated data. E2E tests are excluded by default (`addopts = "-m 'not e2e'"` in `pyproject.toml`) — they generate a fresh dataset via `generate_bank_data.py` or use `data_sample/`.

## Cloud Deployment (Databricks on AWS)

### Architecture

```
┌───────────────────────────────────────────────────────────┐
│                     AWS Account                           │
│                                                           │
│  ┌──────────────┐    ┌──────────────┐                     │
│  │ S3: Raw Data │    │ S3: Delta    │                     │
│  │ ENROLLMENT/  │    │ bronze/      │                     │
│  │ MAINTENANCE/ │    │ silver/      │                     │
│  │ (csv.gz)     │    │ gold/        │                     │
│  └──────┬───────┘    └──────▲───────┘                     │
│         │                   │                             │
│         │            ┌──────┴───────┐                     │
│         └───────────►│  Databricks  │                     │
│                      │  Multi-task  │                     │
│                      │  Job         │                     │
│                      │              │                     │
│                      │ Bronze ──►   │  Pipeline code      │
│                      │ Silver ──►   │  stored on DBFS     │
│                      │ Gold   ──►   │                     │
│                      └──────────────┘                     │
└───────────────────────────────────────────────────────────┘
```

S3 access is configured via Hadoop S3A credentials passed through the cluster's `spark_conf`. The Databricks cross-account role is granted an inline S3 policy by Terraform for bucket access.

### Prerequisites

- AWS CLI configured with credentials
- Terraform >= 1.5
- Databricks CLI configured (for DBFS uploads via `make push-code`)
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

Terraform manages these IAM resources:

| Resource | Purpose |
|----------|---------|
| **S3 buckets** (x2) | Raw data bucket + Delta Lake bucket (versioning enabled on Delta) |
| **S3 access policy** on `databricks-compute-role-*` | Grants the Databricks cross-account role `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on both buckets |
| **ECR repository** | Container registry (provisioned for future use with Databricks Container Services) |

The Databricks job cluster also receives AWS credentials via `spark.hadoop.fs.s3a.*` spark conf so that Spark can read/write S3 directly. These are the same credentials used for `aws configure`.

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

# 2. Deploy infrastructure (S3, IAM policy, Databricks job)
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

### Assumptions

- The interviewer has an existing Databricks workspace on AWS. Terraform creates resources *within* it (jobs, clusters) plus AWS resources (S3, IAM policy) — it does not create the workspace itself.
- S3 bucket names are configurable via `infra/variables.tf`.
- The job cluster uses the standard Databricks runtime (14.3 LTS). Pipeline code is uploaded to DBFS and does not require Docker or custom containers.
- S3 credentials are passed to the cluster via `spark.hadoop.fs.s3a.*` conf. For production, these should be replaced with instance profiles or Unity Catalog storage credentials.

### Bonus Challenges

| Challenge | How Addressed |
|-----------|---------------|
| **Incremental Processing** | Gold cloud entrypoint uses Delta Lake MERGE (upsert by `account_number + phone_number`) instead of full overwrite. |
| **Data Quality Monitoring** | `pipeline/data_quality.py` runs checks after each layer (null counts, uniqueness, format validation). Results logged; failures optionally halt the job. |
| **Audit Trail** | Delta Change Data Feed enabled globally via Terraform `spark_conf`. Supports `SELECT * FROM table_changes(...)` and time travel queries. |
| **Real-time API** | Gold Delta table queryable via Databricks SQL Warehouse (<5s). For sub-100ms, a DynamoDB + Lambda layer could be added (documented stretch goal). |

---

## Local Development

### Pipeline Layers

**Bronze (ingestion)** — Read raw gzipped pipe-delimited CSVs into Spark DataFrames with file-date metadata extracted from filenames.

**Silver (reconciliation)** — Reconcile the three sources into one row per `(account_number, phone_number)`:
- Detect hard deletes (phone absent from latest NON-FIN snapshot)
- Detect soft deletes (SUPPLFWD `cnsmr_phn_sft_dlt_flg = 'Y'`)
- Handle re-adds (soft-deleted phone reappearing in a later snapshot)
- Track consent status from the most recent source
- Resolve NON-FIN vs SUPPLFWD conflicts (most recent event date wins; SUPPLFWD wins ties)
- Include enrollment-only phones (phones in ENRLMT but never in NON-FIN or SUPPLFWD)
- Derive `account_number` from legacy identifier

**Gold (compliance)** — Transform Silver output into the final `phone_consent` table:
- Normalize phone numbers to `+1XXXXXXXXXX` format
- Convert consent flags (`Y`/`N`/NULL) to boolean `has_sms_consent`
- Expose `can_send_sms(account, phone)` compliance API (returns `True` only for CLIENT-sourced, non-deleted phones with consent not withdrawn)

### Key Design Decisions

- **Hard delete detection**: Single `groupBy` + `max(_file_date)` per phone vs global max snapshot date. Avoids expensive pairwise snapshot comparisons.
- **Merge strategy**: Union NON-FIN and SUPPLFWD into a common schema, then `row_number()` window to pick the most recent record. More efficient than a full outer join.
- **Enrollment fallback**: Enrollment-only phones (never in NON-FIN or SUPPLFWD) are included via `left_anti` join — they only fill gaps, never override fresher data.
- **Account number**: Derived in Silver by stripping the trailing "P" from `cnsmr_idntfr_lgcy_txt`.
- **Implicit consent**: `can_send_sms` treats NULL consent as permissive (implicit consent allowed) per business rules. Only an explicit `N` (consent withdrawn) blocks SMS outreach.
- **Account-phone independence**: Each `(account_number, phone_number)` pair is evaluated independently. The same phone number on different accounts can have different deletion and consent states.

### Edge Cases Handled

| Edge Case | Approach |
|-----------|----------|
| Same phone on multiple accounts | Each `(account, phone)` pair tracked independently through all layers |
| Phone re-added after deletion | If winning NON-FIN record is newer than the soft-delete date, the soft delete is overridden |
| Consent changes over time | Latest record (by event date) determines current consent status |
| Missing consent flag | Treated as implicit consent (NULL) — `can_send_sms` returns True |
| SUPPLFWD vs NON-FIN conflicts | Most recent event date wins; SUPPLFWD breaks ties |
| Duplicate records in same file | Deduplicated by `phone_id` (NON-FIN) or `(legacy_id, phone, record_date)` (SUPPLFWD) |
| Enrollment-only phones | Included in Silver/Gold if never seen in NON-FIN or SUPPLFWD |

### Gold Schema (`phone_consent` table)

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

### `can_send_sms` Rules

Returns **True** only when all conditions are met:
1. The `(account_number, phone_number)` row exists in `phone_consent`
2. `phone_source = 'CLIENT'`
3. `is_deleted = False`
4. `has_sms_consent` is `True` or `NULL` (implicit consent allowed)

Returns **False** otherwise (including when no matching row is found).

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
  databricks.tf            # Multi-task job: Bronze → Silver → Gold with S3A credentials
  outputs.tf               # Bucket names, ECR URL, job ID
  terraform.tfvars.example # Example configuration
Makefile                   # push-data, build, push-image, deploy, run-pipeline, test
Dockerfile                 # Databricks-compatible image (optional, for Container Services)
generate_bank_data.py      # Full dataset generator (configurable size/seed)
generate_sample_data.py    # Sample data + preview generator for local dev
```
