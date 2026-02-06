"""Tests for pipeline.bronze — column schemas, file-date parsing, and row counts."""

import datetime

from pyspark.sql.types import DateType, StringType

from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd

# -- Expected columns (data columns only, excluding _source_file / _file_date) --

ENROLLMENT_COLUMNS = [
    "account_number",
    "consumer_legacy_identifier",
    "agency_id",
    "ssn",
    "first_name",
    "last_name",
    "phone_number",
    "phone_type",
    "phone_technology",
    "phone_status",
    "phone_source",
    "quality_score",
    "enrollment_date",
]

NONFIN_COLUMNS = [
    "cnsmr_phn_id",
    "cnsmr_idntfr_lgcy_txt",
    "cnsmr_idntfr_agncy_id",
    "cnsmr_idntfr_ssn_txt",
    "cnsmr_nm_frst_txt",
    "cnsmr_nm_lst_txt",
    "cnsmr_phn_nmbr_txt",
    "cnsmr_phn_typ_val_txt",
    "cnsmr_phn_stts_val_txt",
    "cnsmr_phn_src_val_txt",
    "cnsmr_phn_tchnlgy_typ_val_txt",
    "cnsmr_phn_qlty_score_nmbr",
    "cnsmr_phn_cnsnt_flg",
    "cnsmr_phn_cnsnt_dt",
]

SUPPLFWD_COLUMNS = NONFIN_COLUMNS + [
    "cnsmr_phn_sft_dlt_flg",
    "record_date",
]
# SUPPLFWD uses cnsmr_phn_hst_id instead of cnsmr_phn_id
SUPPLFWD_COLUMNS = ["cnsmr_phn_hst_id"] + SUPPLFWD_COLUMNS[1:]

METADATA_COLUMNS = ["_source_file", "_file_date"]


# ---- Column schema tests ----


class TestEnrollmentSchema:
    def test_has_expected_columns(self, spark, sample_data_dir):
        df = ingest_enrollment(spark, str(sample_data_dir))
        expected = ENROLLMENT_COLUMNS + METADATA_COLUMNS
        assert df.columns == expected

    def test_row_count(self, spark, sample_data_dir):
        df = ingest_enrollment(spark, str(sample_data_dir))
        assert df.count() == 2

    def test_data_columns_are_strings(self, spark, sample_data_dir):
        df = ingest_enrollment(spark, str(sample_data_dir))
        for col_name in ENROLLMENT_COLUMNS:
            assert df.schema[col_name].dataType == StringType()


class TestNonfinSchema:
    def test_has_expected_columns(self, spark, sample_data_dir):
        df = ingest_nonfin(spark, str(sample_data_dir))
        expected = NONFIN_COLUMNS + METADATA_COLUMNS
        assert df.columns == expected

    def test_row_count(self, spark, sample_data_dir):
        df = ingest_nonfin(spark, str(sample_data_dir))
        assert df.count() == 2

    def test_data_columns_are_strings(self, spark, sample_data_dir):
        df = ingest_nonfin(spark, str(sample_data_dir))
        for col_name in NONFIN_COLUMNS:
            assert df.schema[col_name].dataType == StringType()


class TestSupplfwdSchema:
    def test_has_expected_columns(self, spark, sample_data_dir):
        df = ingest_supplfwd(spark, str(sample_data_dir))
        expected = SUPPLFWD_COLUMNS + METADATA_COLUMNS
        assert df.columns == expected

    def test_row_count(self, spark, sample_data_dir):
        df = ingest_supplfwd(spark, str(sample_data_dir))
        assert df.count() == 2

    def test_data_columns_are_strings(self, spark, sample_data_dir):
        df = ingest_supplfwd(spark, str(sample_data_dir))
        for col_name in SUPPLFWD_COLUMNS:
            assert df.schema[col_name].dataType == StringType()


# ---- File-date parsing tests ----


class TestFileDateParsing:
    def test_enrollment_file_date(self, spark, sample_data_dir):
        df = ingest_enrollment(spark, str(sample_data_dir))
        assert df.schema["_file_date"].dataType == DateType()
        dates = [row._file_date for row in df.select("_file_date").distinct().collect()]
        assert dates == [datetime.date(2019, 1, 1)]

    def test_nonfin_file_date(self, spark, sample_data_dir):
        df = ingest_nonfin(spark, str(sample_data_dir))
        assert df.schema["_file_date"].dataType == DateType()
        dates = [row._file_date for row in df.select("_file_date").distinct().collect()]
        assert dates == [datetime.date(2019, 2, 6)]

    def test_supplfwd_file_date(self, spark, sample_data_dir):
        df = ingest_supplfwd(spark, str(sample_data_dir))
        assert df.schema["_file_date"].dataType == DateType()
        dates = [row._file_date for row in df.select("_file_date").distinct().collect()]
        assert dates == [datetime.date(2019, 3, 15)]

    def test_file_date_never_null(self, spark, sample_data_dir):
        for ingest_fn in [ingest_enrollment, ingest_nonfin, ingest_supplfwd]:
            df = ingest_fn(spark, str(sample_data_dir))
            null_count = df.filter(df._file_date.isNull()).count()
            assert null_count == 0, f"{ingest_fn.__name__} produced null _file_date"

    def test_source_file_populated(self, spark, sample_data_dir):
        for ingest_fn in [ingest_enrollment, ingest_nonfin, ingest_supplfwd]:
            df = ingest_fn(spark, str(sample_data_dir))
            empty_count = df.filter(
                (df._source_file.isNull()) | (df._source_file == "")
            ).count()
            assert empty_count == 0, f"{ingest_fn.__name__} has empty _source_file"
