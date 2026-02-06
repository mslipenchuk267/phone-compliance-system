"""Shared fixtures for the test suite."""

import datetime
import gzip
import os
import sys
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


# ---------------------------------------------------------------------------
# Headers reused across Silver fixtures
# ---------------------------------------------------------------------------

_NONFIN_HEADER = (
    "cnsmr_phn_id|cnsmr_idntfr_lgcy_txt|cnsmr_idntfr_agncy_id|cnsmr_idntfr_ssn_txt"
    "|cnsmr_nm_frst_txt|cnsmr_nm_lst_txt|cnsmr_phn_nmbr_txt|cnsmr_phn_typ_val_txt"
    "|cnsmr_phn_stts_val_txt|cnsmr_phn_src_val_txt|cnsmr_phn_tchnlgy_typ_val_txt"
    "|cnsmr_phn_qlty_score_nmbr|cnsmr_phn_cnsnt_flg|cnsmr_phn_cnsnt_dt"
)

_SUPPLFWD_HEADER = (
    "cnsmr_phn_hst_id|cnsmr_idntfr_lgcy_txt|cnsmr_idntfr_agncy_id|cnsmr_idntfr_ssn_txt"
    "|cnsmr_nm_frst_txt|cnsmr_nm_lst_txt|cnsmr_phn_nmbr_txt|cnsmr_phn_typ_val_txt"
    "|cnsmr_phn_stts_val_txt|cnsmr_phn_src_val_txt|cnsmr_phn_tchnlgy_typ_val_txt"
    "|cnsmr_phn_qlty_score_nmbr|cnsmr_phn_cnsnt_flg|cnsmr_phn_cnsnt_dt"
    "|cnsmr_phn_sft_dlt_flg|record_date"
)

_ENRLMT_HEADER = (
    "account_number|consumer_legacy_identifier|agency_id|ssn|first_name|last_name"
    "|phone_number|phone_type|phone_technology|phone_status|phone_source"
    "|quality_score|enrollment_date"
)


@pytest.fixture()
def silver_sample_data_dir(tmp_path):
    """Create multi-snapshot test data for Silver layer tests.

    Scenarios:
      Account A (1111111111111111P) — 2 phones
        A1 (5551111111): In snapshots 1,2 but NOT 3 → hard delete
        A2 (5552222222): In all 3 snapshots; consent Y→N → consent withdrawal
          Snapshot 1 has a DUPLICATE row (same phn_id, different consent_dt) to test dedup

      Account B (2222222222222222P) — 1 phone
        B1 (5553333333): In all 3 snapshots; SUPPLFWD soft-delete AFTER snapshot 3 → soft delete

      Account C (3333333333333333P) — 1 phone
        C1 (5554444444): Soft-deleted via SUPPLFWD at Feb 15, re-appears in snapshot 3 (Mar) → not deleted

      Account D (4444444444444444P) — 1 phone
        D1 (5555555555): Only in SUPPLFWD, never in NON-FIN → not deleted, SUPPLFWD source
          SUPPLFWD has a DUPLICATE row (same key, different hist_id) to test dedup

      Account E (5555555555555555P) — 1 phone
        E1 (5556666666): Only in ENRLMT, never in NON-FIN or SUPPLFWD → enrollment-only

      Account F (6666666666666666P) — 1 phone
        F1 (5554444444): Same phone as C1 but different account; in snapshots 1,2 NOT 3 → hard delete

      Account G (7777777777777777P) — 1 phone
        G1 (5557777777): In NON-FIN snapshot 3 AND SUPPLFWD with SAME date (2019-03-01)
          NON-FIN: source=CONSUMER, consent=Y
          SUPPLFWD: source=CLIENT, consent=N → SUPPLFWD wins tie
    """
    maint = tmp_path / "MAINTENANCE"
    enrl = tmp_path / "ENROLLMENT"

    # --- NON-FIN Snapshot 1: 2019-01-01 ---
    _write_gz_csv(
        maint / "SN_SVC_DMBDL_DataManager-20190101000000100-NON-FIN-BDL_EXPORT_000.csv.gz",
        _NONFIN_HEADER,
        [
            # A1 — present (will disappear in snapshot 3)
            "aaa1-0001|1111111111111111P|AGN000001|111111111|Alice|Smith|5551111111|CELL|ACTIVE|CLIENT|WIRELESS|90||",
            # A2 — present, consent=Y
            "aaa2-0001|1111111111111111P|AGN000001|111111111|Alice|Smith|5552222222|HOME|ACTIVE|CLIENT|LANDLINE|80|Y|2019-01-01",
            # A2 — DUPLICATE (same phn_id + file_date, different consent_dt) to test dedup
            "aaa2-0001|1111111111111111P|AGN000001|111111111|Alice|Smith|5552222222|HOME|ACTIVE|CLIENT|LANDLINE|80|Y|2019-01-10",
            # B1 — present
            "bbb1-0001|2222222222222222P|AGN000002|222222222|Bob|Jones|5553333333|CELL|ACTIVE|THIRD_PARTY|WIRELESS|70||",
            # C1 — present (will be soft-deleted then re-added)
            "ccc1-0001|3333333333333333P|AGN000003|333333333|Carol|Davis|5554444444|WORK|ACTIVE|CLIENT|VOIP|60||",
            # F1 — same phone as C1, different account (will be hard deleted)
            "fff1-0001|6666666666666666P|AGN000006|666666666|Eve|Taylor|5554444444|HOME|ACTIVE|OTHER|LANDLINE|50||",
        ],
    )

    # --- NON-FIN Snapshot 2: 2019-02-01 ---
    _write_gz_csv(
        maint / "SN_SVC_DMBDL_DataManager-20190201000000200-NON-FIN-BDL_EXPORT_001.csv.gz",
        _NONFIN_HEADER,
        [
            # A1 — still present
            "aaa1-0001|1111111111111111P|AGN000001|111111111|Alice|Smith|5551111111|CELL|ACTIVE|CLIENT|WIRELESS|90||",
            # A2 — consent still Y
            "aaa2-0001|1111111111111111P|AGN000001|111111111|Alice|Smith|5552222222|HOME|ACTIVE|CLIENT|LANDLINE|80|Y|2019-01-01",
            # B1 — still present
            "bbb1-0001|2222222222222222P|AGN000002|222222222|Bob|Jones|5553333333|CELL|ACTIVE|THIRD_PARTY|WIRELESS|70||",
            # C1 — ABSENT (soft-deleted between snap 1 and 2, will re-appear in snap 3)
            # F1 — still present (will disappear in snap 3 → hard delete)
            "fff1-0001|6666666666666666P|AGN000006|666666666|Eve|Taylor|5554444444|HOME|ACTIVE|OTHER|LANDLINE|50||",
        ],
    )

    # --- NON-FIN Snapshot 3: 2019-03-01 ---
    _write_gz_csv(
        maint / "SN_SVC_DMBDL_DataManager-20190301000000300-NON-FIN-BDL_EXPORT_002.csv.gz",
        _NONFIN_HEADER,
        [
            # A1 — ABSENT → hard delete (last seen 2019-02-01)
            # A2 — consent changed to N
            "aaa2-0001|1111111111111111P|AGN000001|111111111|Alice|Smith|5552222222|HOME|ACTIVE|CLIENT|LANDLINE|80|N|2019-02-20",
            # B1 — still present
            "bbb1-0001|2222222222222222P|AGN000002|222222222|Bob|Jones|5553333333|CELL|ACTIVE|THIRD_PARTY|WIRELESS|70||",
            # C1 — re-appears after soft-delete
            "ccc1-0001|3333333333333333P|AGN000003|333333333|Carol|Davis|5554444444|WORK|ACTIVE|CLIENT|VOIP|60||",
            # G1 — same date as SUPPLFWD record (tie-breaking: SUPPLFWD should win)
            "ggg1-0001|7777777777777777P|AGN000007|777777777|Grace|Lee|5557777777|CELL|ACTIVE|CONSUMER|WIRELESS|75|Y|2019-03-01",
        ],
    )

    # --- SUPPLFWD records ---
    # File 1: soft-delete for C1 on 2019-02-15 (before snapshot 3 re-add)
    _write_gz_csv(
        maint / "SN_SVC_DMBDL_DataManager-20190215000000400-SUPPLFWD-BDL_EXPORT_000.csv.gz",
        _SUPPLFWD_HEADER,
        [
            # C1 soft-deleted
            "sf-ccc1-01|3333333333333333P|AGN000003|333333333|Carol|Davis|5554444444|WORK|ACTIVE|CLIENT|VOIP|60|||Y|2019-02-15",
        ],
    )

    # File 2: soft-delete for B1 on 2019-04-01 (AFTER latest NON-FIN snapshot)
    # Also: D1 only appears here, never in NON-FIN
    _write_gz_csv(
        maint / "SN_SVC_DMBDL_DataManager-20190401000000500-SUPPLFWD-BDL_EXPORT_001.csv.gz",
        _SUPPLFWD_HEADER,
        [
            # B1 soft-deleted after all snapshots
            "sf-bbb1-01|2222222222222222P|AGN000002|222222222|Bob|Jones|5553333333|CELL|ACTIVE|THIRD_PARTY|WIRELESS|70|||Y|2019-04-01",
            # D1 only in SUPPLFWD, active, consent Y
            "sf-ddd1-01|4444444444444444P|AGN000004|444444444|Dan|Wilson|5555555555|CELL|ACTIVE|CLIENT|WIRELESS|95|Y|2019-03-01|N|2019-04-01",
            # D1 — DUPLICATE (same legacy_id + phone + record_date, different hist_id) to test dedup
            "sf-ddd1-02|4444444444444444P|AGN000004|444444444|Dan|Wilson|5555555555|CELL|ACTIVE|CLIENT|WIRELESS|90|N|2019-03-01|N|2019-04-01",
            # G1 — same date as NON-FIN snapshot 3 (SUPPLFWD should win tie): source=CLIENT, consent=N
            "sf-ggg1-01|7777777777777777P|AGN000007|777777777|Grace|Lee|5557777777|CELL|ACTIVE|CLIENT|WIRELESS|75|N|2019-03-01|N|2019-03-01",
        ],
    )

    # --- ENROLLMENT ---
    _write_gz_csv(
        enrl / "IC_A2RGMRHG_DMENRL_DataManager-20190101000000600-ENRLMT-EXPORT_000.csv.gz",
        _ENRLMT_HEADER,
        [
            "1111111111111111|1111111111111111P|AGN000001|111111111|Alice|Smith|5551111111|CELL|WIRELESS|ACTIVE|CLIENT|90|2019-01-01",
            "2222222222222222|2222222222222222P|AGN000002|222222222|Bob|Jones|5553333333|CELL|WIRELESS|ACTIVE|THIRD_PARTY|70|2019-01-01",
            "3333333333333333|3333333333333333P|AGN000003|333333333|Carol|Davis|5554444444|WORK|VOIP|ACTIVE|CLIENT|60|2019-01-01",
            "4444444444444444|4444444444444444P|AGN000004|444444444|Dan|Wilson|5555555555|CELL|WIRELESS|ACTIVE|CLIENT|95|2019-01-01",
            "5555555555555555|5555555555555555P|AGN000005|555555555|Emily|Brown|5556666666|CELL|WIRELESS|ACTIVE|CLIENT|88|2019-01-01",
            "6666666666666666|6666666666666666P|AGN000006|666666666|Eve|Taylor|5554444444|HOME|LANDLINE|ACTIVE|OTHER|50|2019-01-01",
            "7777777777777777|7777777777777777P|AGN000007|777777777|Grace|Lee|5557777777|CELL|WIRELESS|ACTIVE|CONSUMER|75|2019-01-01",
        ],
    )

    return tmp_path


# ---------------------------------------------------------------------------
# E2E data source option
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent


def pytest_addoption(parser):
    parser.addoption(
        "--e2e-data",
        default="generate",
        choices=["generate", "sample"],
        help="E2E data source: 'generate' (fresh via generate_bank_data.py) or 'sample' (data_sample/)",
    )


@pytest.fixture(scope="module")
def e2e_data_dir(request, tmp_path_factory):
    """Provide a data directory for E2E tests.

    --e2e-data=generate (default): generate a small dataset into a temp directory.
    --e2e-data=sample: use the existing data_sample/ directory on disk.
    """
    mode = request.config.getoption("--e2e-data")
    if mode == "sample":
        path = _PROJECT_ROOT / "data_sample"
        if not path.is_dir():
            pytest.skip("data_sample/ not found (gitignored, generate locally)")
        return str(path)

    # Import the generator and create a small dataset
    sys.path.insert(0, str(_PROJECT_ROOT))
    from generate_bank_data import generate_dataset

    out = tmp_path_factory.mktemp("e2e_generated")
    generate_dataset(str(out), target_size_gb=0.05, seed=42)
    return str(out)
