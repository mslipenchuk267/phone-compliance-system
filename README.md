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

To work with a smaller subset locally:

```bash
# data_sample/ — small gzipped subset for PySpark
# data_preview/ — same files extracted as plain CSV for manual inspection
```

These directories are gitignored. To regenerate them, copy a few files from `data/` and extract with `gzcat`.

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

### Unit Tests

```bash
uv run pytest tests/ -v
```

Tests use self-contained fixtures (tiny gzipped CSVs written to temp directories) and don't require the generated data. The test suite covers Bronze ingestion (14 tests), Silver reconciliation (31 tests), and Gold compliance logic (21 tests) — 66 tests total.

## Architecture

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
  spark_session.py   # SparkSession factory (local mode, Java 17 config)
  bronze.py          # Bronze layer: read raw files into DataFrames
  silver.py          # Silver layer: reconcile into one row per (account, phone)
  gold.py            # Gold layer: phone_consent table and can_send_sms API
  run_bronze.py      # Bronze pipeline runner
  run_silver.py      # Silver pipeline runner with summary stats
  run_gold.py        # Gold pipeline runner with compliance checks
tests/
  conftest.py        # Shared fixtures (SparkSession, Bronze + Silver sample data)
  test_bronze.py     # Bronze schema, column, type, and file-date parsing tests (14)
  test_silver.py     # Silver dedup, deletes, merging, enrollment, independence (31)
  test_gold.py       # Gold normalization, consent, can_send_sms compliance (21)
data/                # Full generated dataset (gitignored)
data_sample/         # Gzipped subset for local dev (gitignored)
data_preview/        # Extracted CSVs for manual inspection (gitignored)
```
