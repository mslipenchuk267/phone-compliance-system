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

### Smoke Test

Verify PySpark can read all three file types:

```bash
# Against sample data
uv run python -m pipeline.smoke_test ./data_sample

# Against full dataset
uv run python -m pipeline.smoke_test ./data
```

### Unit Tests

```bash
uv run pytest tests/ -v
```

Tests use self-contained fixtures (tiny gzipped CSVs written to temp directories) and don't require the generated data.

## Project Structure

```
pipeline/
  spark_session.py   # SparkSession factory (local mode, Java 17 config)
  ingest.py          # Bronze layer: read raw files into DataFrames
  smoke_test.py      # End-to-end ingestion verification
tests/
  conftest.py        # Shared fixtures (SparkSession, sample data generation)
  test_ingest.py     # Schema, column, type, and file-date parsing tests
data/                # Full generated dataset (gitignored)
data_sample/         # Small gzipped subset for local dev (gitignored)
data_preview/        # Extracted CSVs for manual inspection (gitignored)
```
