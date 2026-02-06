"""Tests for pipeline.gold — phone normalization, consent derivation, and can_send_sms."""

import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd
from pipeline.silver import build_silver_table
from pipeline.gold import (
    build_phone_consent_table,
    can_send_sms,
    derive_sms_consent,
    normalize_phone_number,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_gold(spark, silver_sample_data_dir):
    """Build the full Gold phone_consent table from test fixtures."""
    nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
    supplfwd = ingest_supplfwd(spark, str(silver_sample_data_dir))
    enrl = ingest_enrollment(spark, str(silver_sample_data_dir))
    silver = build_silver_table(nonfin, supplfwd, enrl)
    return build_phone_consent_table(silver)


def _lookup(df, filters: dict):
    """Filter a DataFrame by exact column matches and return the first row as a dict."""
    for col, val in filters.items():
        df = df.filter(F.col(col) == val)
    rows = df.collect()
    assert len(rows) >= 1, f"No rows found for {filters}"
    return rows[0].asDict()


# ---------------------------------------------------------------------------
# TestNormalizePhoneNumber
# ---------------------------------------------------------------------------


class TestNormalizePhoneNumber:
    def test_prepends_plus_one(self, spark):
        df = spark.createDataFrame(
            [("5551111111",), ("3125551234",)], ["phone_number"]
        )
        result = normalize_phone_number(df)
        numbers = sorted([r.phone_number for r in result.collect()])
        assert numbers == ["+13125551234", "+15551111111"]

    def test_result_is_string_type(self, spark):
        df = spark.createDataFrame([("5551111111",)], ["phone_number"])
        result = normalize_phone_number(df)
        assert result.schema["phone_number"].dataType == StringType()


# ---------------------------------------------------------------------------
# TestDeriveSmsConsent
# ---------------------------------------------------------------------------


class TestDeriveSmsConsent:
    def test_y_becomes_true(self, spark):
        df = spark.createDataFrame([("Y",)], ["consent_flag"])
        result = derive_sms_consent(df)
        assert result.collect()[0]["has_sms_consent"] is True

    def test_n_becomes_false(self, spark):
        df = spark.createDataFrame([("N",)], ["consent_flag"])
        result = derive_sms_consent(df)
        assert result.collect()[0]["has_sms_consent"] is False

    def test_null_stays_null(self, spark):
        schema = StructType([StructField("consent_flag", StringType(), True)])
        df = spark.createDataFrame([(None,)], schema=schema)
        result = derive_sms_consent(df)
        assert result.collect()[0]["has_sms_consent"] is None

    def test_empty_string_becomes_null(self, spark):
        df = spark.createDataFrame([("",)], ["consent_flag"])
        result = derive_sms_consent(df)
        assert result.collect()[0]["has_sms_consent"] is None

    def test_result_is_boolean_type(self, spark):
        df = spark.createDataFrame([("Y",)], ["consent_flag"])
        result = derive_sms_consent(df)
        assert result.schema["has_sms_consent"].dataType == BooleanType()


# ---------------------------------------------------------------------------
# TestBuildPhoneConsentTable
# ---------------------------------------------------------------------------


class TestBuildPhoneConsentTable:
    def test_output_columns(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        expected = [
            "account_number",
            "phone_number",
            "phone_source",
            "has_sms_consent",
            "sms_consent_updated_at",
            "is_deleted",
            "delete_type",
            "deleted_at",
        ]
        assert gold.columns == expected

    def test_one_row_per_account_phone(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        total = gold.count()
        distinct = gold.select("account_number", "phone_number").distinct().count()
        assert total == distinct

    def test_phone_numbers_normalized(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        non_prefixed = gold.filter(~F.col("phone_number").startswith("+1")).count()
        assert non_prefixed == 0

    def test_consent_converted(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        # A2 had consent_flag='N' → has_sms_consent=False
        row = _lookup(gold, {"phone_number": "+15552222222"})
        assert row["has_sms_consent"] is False

    def test_null_consent_preserved(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        # A1 had no consent flag → has_sms_consent=NULL
        row = _lookup(gold, {"phone_number": "+15551111111"})
        assert row["has_sms_consent"] is None

    def test_consent_date_renamed(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        row = _lookup(gold, {"phone_number": "+15552222222"})
        assert row["sms_consent_updated_at"] == datetime.date(2019, 2, 20)

    def test_enrollment_only_phone_in_output(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        # E1 — enrollment-only phone appears in Gold with normalized number
        row = _lookup(gold, {"phone_number": "+15556666666"})
        assert row["account_number"] == "5555555555555555"
        assert row["phone_source"] == "CLIENT"
        assert row["is_deleted"] is False
        assert row["has_sms_consent"] is None

    def test_supplfwd_tie_breaking_in_output(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        # G1: SUPPLFWD won the same-date tie → CLIENT source, consent=N→False
        row = _lookup(gold, {"phone_number": "+15557777777"})
        assert row["phone_source"] == "CLIENT"
        assert row["has_sms_consent"] is False
        assert row["is_deleted"] is False

    def test_total_row_count(self, spark, silver_sample_data_dir):
        gold = _build_gold(spark, silver_sample_data_dir)
        assert gold.count() == 8


# ---------------------------------------------------------------------------
# TestCanSendSms
# ---------------------------------------------------------------------------


class TestCanSendSms:
    def _get_consent_table(self, spark, silver_sample_data_dir):
        return _build_gold(spark, silver_sample_data_dir)

    def test_deleted_phone_returns_false(self, spark, silver_sample_data_dir):
        """A1: CLIENT, hard-deleted, NULL consent → False (deleted)."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        assert can_send_sms(gold, "1111111111111111", "+15551111111") is False

    def test_consent_withdrawn_returns_false(self, spark, silver_sample_data_dir):
        """A2: CLIENT, not deleted, consent=False → False (no consent)."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        assert can_send_sms(gold, "1111111111111111", "+15552222222") is False

    def test_third_party_source_returns_false(self, spark, silver_sample_data_dir):
        """B1: THIRD_PARTY, soft-deleted, NULL consent → False (not CLIENT)."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        assert can_send_sms(gold, "2222222222222222", "+15553333333") is False

    def test_client_active_implicit_consent_returns_true(self, spark, silver_sample_data_dir):
        """C1: CLIENT, not deleted, NULL consent → True (implicit consent)."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        assert can_send_sms(gold, "3333333333333333", "+15554444444") is True

    def test_client_active_explicit_consent_returns_true(self, spark, silver_sample_data_dir):
        """D1: CLIENT, not deleted, consent=True → True (explicit consent)."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        assert can_send_sms(gold, "4444444444444444", "+15555555555") is True

    def test_enrollment_only_can_send_sms(self, spark, silver_sample_data_dir):
        """E1: CLIENT, not deleted, NULL consent (enrollment-only) → True."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        assert can_send_sms(gold, "5555555555555555", "+15556666666") is True

    def test_same_phone_independent_per_account(self, spark, silver_sample_data_dir):
        """Same phone 5554444444 on two accounts has independent outcomes."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        # C1 (account C): CLIENT, not deleted, NULL consent → True
        assert can_send_sms(gold, "3333333333333333", "+15554444444") is True
        # F1 (account F): OTHER, hard-deleted → False
        assert can_send_sms(gold, "6666666666666666", "+15554444444") is False

    def test_consent_withdrawn_via_tie_breaking(self, spark, silver_sample_data_dir):
        """G1: CLIENT, not deleted, consent=False (SUPPLFWD won same-date tie) → False."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        assert can_send_sms(gold, "7777777777777777", "+15557777777") is False

    def test_nonexistent_phone_returns_false(self, spark, silver_sample_data_dir):
        """Lookup for a phone that doesn't exist → False."""
        gold = self._get_consent_table(spark, silver_sample_data_dir)
        assert can_send_sms(gold, "9999999999999999", "+19999999999") is False
