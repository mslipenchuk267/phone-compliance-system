#!/usr/bin/env python3
"""Generate data_sample/ and data_preview/ directories for local development.

data_sample/  — small gzipped dataset readable by PySpark
data_preview/ — same files extracted as plain CSV for manual inspection

Both directories are gitignored.

Usage:
    uv run python generate_sample_data.py
"""

import gzip
import os
import shutil

from generate_bank_data import generate_dataset

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
_SAMPLE_DIR = os.path.join(_PROJECT_ROOT, "data_sample")
_PREVIEW_DIR = os.path.join(_PROJECT_ROOT, "data_preview")


def _extract_gz_to_preview(sample_dir: str, preview_dir: str) -> int:
    """Extract all .csv.gz files from sample_dir into preview_dir as plain CSVs."""
    count = 0
    for dirpath, _dirnames, filenames in os.walk(sample_dir):
        for filename in filenames:
            if not filename.endswith(".csv.gz"):
                continue
            gz_path = os.path.join(dirpath, filename)
            # Mirror the subdirectory structure (MAINTENANCE/, ENROLLMENT/)
            rel_dir = os.path.relpath(dirpath, sample_dir)
            out_dir = os.path.join(preview_dir, rel_dir)
            os.makedirs(out_dir, exist_ok=True)
            csv_name = filename.removesuffix(".gz")
            csv_path = os.path.join(out_dir, csv_name)
            with gzip.open(gz_path, "rt") as fin, open(csv_path, "w") as fout:
                fout.write(fin.read())
            count += 1
    return count


def main():
    # Clean existing directories
    for d in (_SAMPLE_DIR, _PREVIEW_DIR):
        if os.path.isdir(d):
            shutil.rmtree(d)

    # Generate sample data (small: ~500 accounts)
    print("Generating data_sample/ ...")
    generate_dataset(_SAMPLE_DIR, target_size_gb=0.005, seed=42)

    # Extract to preview
    print(f"\nExtracting to data_preview/ ...")
    n = _extract_gz_to_preview(_SAMPLE_DIR, _PREVIEW_DIR)
    print(f"Extracted {n} files to {_PREVIEW_DIR}")


if __name__ == "__main__":
    main()
