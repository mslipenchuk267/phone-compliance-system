"""Shared fixtures for the test suite."""

import datetime
import gzip
import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Ensure Java 17 for PySpark compatibility.
_JAVA17_HOME = (
    "/Users/matthewslipenchuk/Library/Java/JavaVirtualMachines"
    "/corretto-17.0.16/Contents/Home"
)
if os.path.isdir(_JAVA17_HOME):
    os.environ["JAVA_HOME"] = _JAVA17_HOME


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for tests."""
    session = (
        SparkSession.builder.appName("phone-compliance-tests")
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _write_gz_csv(path: Path, header: str, rows: list[str]) -> None:
    """Write a gzipped pipe-delimited CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    content = header + "\n" + "\n".join(rows) + "\n"
    with gzip.open(path, "wt") as f:
        f.write(content)


@pytest.fixture()
def sample_data_dir(tmp_path):
    """Create a minimal sample data directory with one file per type.

    Returns the path to the root data directory.
    """
    # --- ENROLLMENT ---
    enrl_header = (
        "account_number|consumer_legacy_identifier|agency_id|ssn|first_name|last_name"
        "|phone_number|phone_type|phone_technology|phone_status|phone_source"
        "|quality_score|enrollment_date"
    )
    enrl_rows = [
        "4423647921499010|4423647921499010P|AGN861394|977602593|Matthew|Taylor"
        "|2125192467|WORK|VOIP|INACTIVE|CONSUMER|85|2019-01-01",
        "5500000000000001|5500000000000001P|AGN100001|111111111|Jane|Doe"
        "|3125551234|CELL|WIRELESS|ACTIVE|CLIENT|92|2019-01-15",
    ]
    _write_gz_csv(
        tmp_path / "ENROLLMENT" / "IC_A2RGMRHG_DMENRL_DataManager-20190101000000863-ENRLMT-EXPORT_000.csv.gz",
        enrl_header,
        enrl_rows,
    )

    # --- NON-FIN-BDL ---
    nonfin_header = (
        "cnsmr_phn_id|cnsmr_idntfr_lgcy_txt|cnsmr_idntfr_agncy_id|cnsmr_idntfr_ssn_txt"
        "|cnsmr_nm_frst_txt|cnsmr_nm_lst_txt|cnsmr_phn_nmbr_txt|cnsmr_phn_typ_val_txt"
        "|cnsmr_phn_stts_val_txt|cnsmr_phn_src_val_txt|cnsmr_phn_tchnlgy_typ_val_txt"
        "|cnsmr_phn_qlty_score_nmbr|cnsmr_phn_cnsnt_flg|cnsmr_phn_cnsnt_dt"
    )
    nonfin_rows = [
        "8db48416-1abe-4dec-ab71-16cdb33f5944|0433218196001338P|AGN731262|913814257"
        "|Amanda|Adams|9544841106|HOME|DISCONNECTED|OTHER|LANDLINE|36||",
        "11f7c9f2-b779-4d34-bb9a-6ed1e329d399|0433218196001338P|AGN731262|913814257"
        "|Amanda|Adams|3125892584|WORK|INACTIVE|THIRD_PARTY|WIRELESS|94|Y|2019-06-15",
    ]
    _write_gz_csv(
        tmp_path / "MAINTENANCE" / "SN_SVC_DMBDL_DataManager-20190206000000539-NON-FIN-BDL_EXPORT_001.csv.gz",
        nonfin_header,
        nonfin_rows,
    )

    # --- SUPPLFWD ---
    suppl_header = (
        "cnsmr_phn_hst_id|cnsmr_idntfr_lgcy_txt|cnsmr_idntfr_agncy_id|cnsmr_idntfr_ssn_txt"
        "|cnsmr_nm_frst_txt|cnsmr_nm_lst_txt|cnsmr_phn_nmbr_txt|cnsmr_phn_typ_val_txt"
        "|cnsmr_phn_stts_val_txt|cnsmr_phn_src_val_txt|cnsmr_phn_tchnlgy_typ_val_txt"
        "|cnsmr_phn_qlty_score_nmbr|cnsmr_phn_cnsnt_flg|cnsmr_phn_cnsnt_dt"
        "|cnsmr_phn_sft_dlt_flg|record_date"
    )
    suppl_rows = [
        "55b008ac-6485-42bb-a2d3-570af1d5d3f3|8771906594013990P|AGN901909|996441659"
        "|Edward|Lopez|8324843973|OTHER|DISCONNECTED|OTHER|WIRELESS|61|||N|2019-01-01",
        "07eacc6a-aea7-4fe2-bfe8-510e1c737ad8|5339636057662702P|AGN626690|985522644"
        "|Mark|Miller|7135677141|OTHER|ACTIVE|CONSUMER|VOIP|53|Y|2019-08-18|N|2019-01-01",
    ]
    _write_gz_csv(
        tmp_path / "MAINTENANCE" / "SN_SVC_DMBDL_DataManager-20190315000000733-SUPPLFWD-BDL_EXPORT_002.csv.gz",
        suppl_header,
        suppl_rows,
    )

    return tmp_path
