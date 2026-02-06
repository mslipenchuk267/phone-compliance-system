"""End-to-end tests: run the full pipeline against generated or sample data.

Use --e2e-data=generate (default) to create a fresh small dataset, or
--e2e-data=sample to use the existing data_sample/ directory.
"""

import pytest
from pyspark.sql import functions as F

from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd
from pipeline.silver import build_silver_table
from pipeline.gold import build_phone_consent_table, can_send_sms

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Fixture: build Gold table once per module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def gold_table(spark, e2e_data_dir):
    """Build Gold table from E2E data (expensive, cached per module)."""
    nonfin = ingest_nonfin(spark, e2e_data_dir)
    supplfwd = ingest_supplfwd(spark, e2e_data_dir)
    enrl = ingest_enrollment(spark, e2e_data_dir)
    silver = build_silver_table(nonfin, supplfwd, enrl)
    gold = build_phone_consent_table(silver)
    gold.cache()
    gold.count()  # Force materialization
    return gold


# ---------------------------------------------------------------------------
# Data quality assertions
# ---------------------------------------------------------------------------


class TestDataQuality:
    def test_row_count_in_expected_range(self, gold_table):
        count = gold_table.count()
        assert 1_000 < count < 1_000_000, f"Unexpected Gold row count: {count}"

    def test_no_duplicate_account_phone_pairs(self, gold_table):
        total = gold_table.count()
        distinct = gold_table.select("account_number", "phone_number").distinct().count()
        assert total == distinct

    def test_all_phone_numbers_normalized(self, gold_table):
        bad = gold_table.filter(~F.col("phone_number").rlike(r"^\+1\d{10}$")).count()
        assert bad == 0, f"{bad} phone numbers don't match +1XXXXXXXXXX format"

    def test_no_null_account_numbers(self, gold_table):
        null_count = gold_table.filter(F.col("account_number").isNull()).count()
        assert null_count == 0

    def test_phone_source_values_valid(self, gold_table):
        sources = {
            row.phone_source
            for row in gold_table.select("phone_source").distinct().collect()
        }
        assert sources.issubset({"CLIENT", "CONSUMER", "THIRD_PARTY", "OTHER"})

    def test_consent_values_valid(self, gold_table):
        values = {
            row.has_sms_consent
            for row in gold_table.select("has_sms_consent").distinct().collect()
        }
        assert values.issubset({True, False, None})

    def test_delete_type_values_valid(self, gold_table):
        values = {
            row.delete_type
            for row in gold_table.select("delete_type").distinct().collect()
        }
        assert values.issubset({"hard_delete", "soft_delete", None})


# ---------------------------------------------------------------------------
# Consistency checks
# ---------------------------------------------------------------------------


class TestConsistency:
    def test_deleted_rows_have_metadata(self, gold_table):
        """Deleted rows must have both delete_type and deleted_at populated."""
        bad = gold_table.filter(
            (F.col("is_deleted") == True)
            & (F.col("delete_type").isNull() | F.col("deleted_at").isNull())
        ).count()
        assert bad == 0, f"{bad} deleted rows missing delete_type or deleted_at"

    def test_non_deleted_rows_clean(self, gold_table):
        """Non-deleted rows must not have delete metadata."""
        bad = gold_table.filter(
            (F.col("is_deleted") == False)
            & (F.col("delete_type").isNotNull() | F.col("deleted_at").isNotNull())
        ).count()
        assert bad == 0, f"{bad} non-deleted rows have unexpected delete metadata"


# ---------------------------------------------------------------------------
# Business logic at scale
# ---------------------------------------------------------------------------


class TestBusinessLogic:
    def test_can_send_sms_consistency(self, gold_table):
        """Spot-check can_send_sms against raw Gold data for a sample of rows."""
        sample = gold_table.limit(50).collect()
        for row in sample:
            result = can_send_sms(
                gold_table, row["account_number"], row["phone_number"]
            )
            expected = (
                row["phone_source"] == "CLIENT"
                and row["is_deleted"] is False
                and row["has_sms_consent"] is not False
            )
            assert result == expected, (
                f"Mismatch for ({row['account_number']}, {row['phone_number']}): "
                f"got {result}, expected {expected}"
            )

    def test_client_source_ratio(self, gold_table):
        """At least 10% of records should be CLIENT-sourced."""
        total = gold_table.count()
        client = gold_table.filter(F.col("phone_source") == "CLIENT").count()
        ratio = client / total
        assert ratio > 0.10, f"CLIENT source ratio {ratio:.2%} is suspiciously low"
