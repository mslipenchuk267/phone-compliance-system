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

### Unit Tests

```bash
uv run pytest tests/ -v
```

Tests use self-contained fixtures (tiny gzipped CSVs written to temp directories) and don't require the generated data. The Silver test suite covers hard deletes, soft deletes, re-adds after deletion, consent withdrawal, SUPPLFWD vs NON-FIN conflict resolution, and SUPPLFWD-only phones.

## Architecture

### Pipeline Layers

**Bronze (ingestion)** — Read raw gzipped pipe-delimited CSVs into Spark DataFrames with file-date metadata extracted from filenames.

**Silver (reconciliation)** — Reconcile the three sources into one row per `(account_number, phone_number)`:
- Detect hard deletes (phone absent from latest NON-FIN snapshot)
- Detect soft deletes (SUPPLFWD `cnsmr_phn_sft_dlt_flg = 'Y'`)
- Handle re-adds (soft-deleted phone reappearing in a later snapshot)
- Track consent status from the most recent source
- Resolve NON-FIN vs SUPPLFWD conflicts (most recent event date wins; SUPPLFWD wins ties)
- Derive `account_number` from legacy identifier

**Gold (TODO)** — `phone_consent` table and `can_send_sms()` compliance API.

### Key Design Decisions

- **Hard delete detection**: Single `groupBy` + `max(_file_date)` per phone vs global max snapshot date. Avoids expensive pairwise snapshot comparisons.
- **Merge strategy**: Union NON-FIN and SUPPLFWD into a common schema, then `row_number()` window to pick the most recent record. More efficient than a full outer join.
- **Account number**: Derived in Silver by stripping the trailing "P" from `cnsmr_idntfr_lgcy_txt`.

## Project Structure

```
pipeline/
  spark_session.py   # SparkSession factory (local mode, Java 17 config)
  bronze.py          # Bronze layer: read raw files into DataFrames
  silver.py          # Silver layer: reconcile into one row per (account, phone)
  run_bronze.py      # Bronze pipeline runner
  run_silver.py      # Silver pipeline runner with summary stats
tests/
  conftest.py        # Shared fixtures (SparkSession, Bronze + Silver sample data)
  test_bronze.py     # Bronze schema, column, type, and file-date parsing tests
  test_silver.py     # Silver dedup, hard/soft deletes, merging, consent, integration
data/                # Full generated dataset (gitignored)
data_sample/         # Gzipped subset for local dev (gitignored)
data_preview/        # Extracted CSVs for manual inspection (gitignored)
```
