"""Integration tests: cross-layer contracts, idempotency, and full pipeline validation."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, StringType

from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd
from pipeline.silver import build_silver_table
from pipeline.gold import build_phone_consent_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_silver(spark, data_dir):
    nonfin = ingest_nonfin(spark, str(data_dir))
    supplfwd = ingest_supplfwd(spark, str(data_dir))
    enrl = ingest_enrollment(spark, str(data_dir))
    return build_silver_table(nonfin, supplfwd, enrl)


# ---------------------------------------------------------------------------
# TestBronzeSilverContract
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestBronzeSilverContract:
    """Bronze output must contain the columns that Silver references."""

    def test_nonfin_has_columns_silver_expects(self, spark, sample_data_dir):
        nonfin = ingest_nonfin(spark, str(sample_data_dir))
        required = {
            "cnsmr_phn_id",
            "cnsmr_idntfr_lgcy_txt",
            "cnsmr_phn_nmbr_txt",
            "cnsmr_phn_typ_val_txt",
            "cnsmr_phn_stts_val_txt",
            "cnsmr_phn_src_val_txt",
            "cnsmr_phn_tchnlgy_typ_val_txt",
            "cnsmr_phn_qlty_score_nmbr",
            "cnsmr_phn_cnsnt_flg",
            "cnsmr_phn_cnsnt_dt",
            "_file_date",
            "_source_file",
        }
        assert required.issubset(set(nonfin.columns))

    def test_supplfwd_has_columns_silver_expects(self, spark, sample_data_dir):
        supplfwd = ingest_supplfwd(spark, str(sample_data_dir))
        required = {
            "cnsmr_phn_hst_id",
            "cnsmr_idntfr_lgcy_txt",
            "cnsmr_phn_nmbr_txt",
            "cnsmr_phn_typ_val_txt",
            "cnsmr_phn_stts_val_txt",
            "cnsmr_phn_src_val_txt",
            "cnsmr_phn_tchnlgy_typ_val_txt",
            "cnsmr_phn_qlty_score_nmbr",
            "cnsmr_phn_cnsnt_flg",
            "cnsmr_phn_cnsnt_dt",
            "cnsmr_phn_sft_dlt_flg",
            "record_date",
            "_file_date",
        }
        assert required.issubset(set(supplfwd.columns))

    def test_enrollment_has_columns_silver_expects(self, spark, sample_data_dir):
        enrl = ingest_enrollment(spark, str(sample_data_dir))
        required = {
            "consumer_legacy_identifier",
            "phone_number",
            "phone_type",
            "phone_status",
            "phone_source",
            "phone_technology",
            "quality_score",
            "enrollment_date",
        }
        assert required.issubset(set(enrl.columns))


# ---------------------------------------------------------------------------
# TestSilverGoldContract
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSilverGoldContract:
    """Silver output must satisfy Gold's input expectations."""

    def test_silver_output_has_columns_gold_expects(self, spark, silver_sample_data_dir):
        silver = _build_silver(spark, silver_sample_data_dir)
        gold_required = {
            "account_number",
            "phone_number",
            "phone_source",
            "consent_flag",
            "consent_date",
            "is_deleted",
            "delete_type",
            "deleted_at",
        }
        assert gold_required.issubset(set(silver.columns))

    def test_silver_column_types_match_gold_expectations(self, spark, silver_sample_data_dir):
        silver = _build_silver(spark, silver_sample_data_dir)
        schema = {f.name: f.dataType for f in silver.schema.fields}
        assert isinstance(schema["is_deleted"], BooleanType)
        assert isinstance(schema["consent_flag"], StringType)
        assert isinstance(schema["consent_date"], DateType)
        assert isinstance(schema["deleted_at"], DateType)

    def test_gold_row_count_matches_silver(self, spark, silver_sample_data_dir):
        """Gold produces exactly 1 row per Silver row — no rows created or dropped."""
        silver = _build_silver(spark, silver_sample_data_dir)
        gold = build_phone_consent_table(silver)
        assert gold.count() == silver.count()


# ---------------------------------------------------------------------------
# TestIdempotency
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIdempotency:
    def test_silver_is_deterministic(self, spark, silver_sample_data_dir):
        """Building Silver twice from the same Bronze inputs produces identical results."""
        silver1 = _build_silver(spark, silver_sample_data_dir)
        silver2 = _build_silver(spark, silver_sample_data_dir)
        diff = silver1.exceptAll(silver2).count() + silver2.exceptAll(silver1).count()
        assert diff == 0


# ---------------------------------------------------------------------------
# TestPipelineEndToEnd
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestPipelineEndToEnd:
    def test_full_pipeline_produces_valid_gold(self, spark, silver_sample_data_dir):
        """Run Bronze → Silver → Gold on fixtures and validate structural invariants."""
        silver = _build_silver(spark, silver_sample_data_dir)
        gold = build_phone_consent_table(silver)

        # Correct column count
        assert len(gold.columns) == 9

        # No null account numbers
        assert gold.filter(F.col("account_number").isNull()).count() == 0

        # All phone numbers normalized
        assert gold.filter(~F.col("phone_number").rlike(r"^\+1\d{10}$")).count() == 0

        # One row per (account, phone)
        total = gold.count()
        distinct = gold.select("account_number", "phone_number").distinct().count()
        assert total == distinct
