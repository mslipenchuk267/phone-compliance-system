"""Tests for pipeline.silver — dedup, hard/soft deletes, merging, and build_silver_table."""

import datetime

from pyspark.sql import functions as F

from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd
from pipeline.silver import (
    build_silver_table,
    dedup_nonfin,
    dedup_supplfwd,
    derive_account_number,
    detect_hard_deletes,
    detect_soft_deletes,
    get_latest_nonfin_state,
    get_latest_supplfwd_state,
    merge_nonfin_and_supplfwd,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _collect_col(df, col):
    """Collect a single column as a sorted list of values."""
    return sorted([row[col] for row in df.select(col).collect()])


def _lookup(df, filters: dict):
    """Filter a DataFrame by exact column matches and return the first row as a dict."""
    for col, val in filters.items():
        df = df.filter(F.col(col) == val)
    rows = df.collect()
    assert len(rows) >= 1, f"No rows found for {filters}"
    return rows[0].asDict()


# ---------------------------------------------------------------------------
# TestDeriveAccountNumber
# ---------------------------------------------------------------------------


class TestDeriveAccountNumber:
    def test_strips_trailing_character(self, spark):
        df = spark.createDataFrame(
            [("1111111111111111P",), ("2222222222222222P",)],
            ["legacy_identifier"],
        )
        result = derive_account_number(df)
        accts = _collect_col(result, "account_number")
        assert accts == ["1111111111111111", "2222222222222222"]

    def test_handles_different_suffixes(self, spark):
        df = spark.createDataFrame([("ABCX",)], ["legacy_identifier"])
        result = derive_account_number(df)
        assert result.collect()[0]["account_number"] == "ABC"


# ---------------------------------------------------------------------------
# TestDedupNonfin
# ---------------------------------------------------------------------------


class TestDedupNonfin:
    def test_removes_duplicates_within_snapshot(self, spark, silver_sample_data_dir):
        nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
        original_count = nonfin.count()
        deduped = dedup_nonfin(nonfin)
        # Fixture has 1 duplicate A2 in snapshot 1 (same phn_id + file_date) → dedup removes it
        assert deduped.count() == original_count - 1

    def test_preserves_across_snapshots(self, spark, silver_sample_data_dir):
        nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
        deduped = dedup_nonfin(nonfin)
        # Phone aaa2-0001 appears in all 3 snapshots (snapshot 1 dupe removed by dedup)
        a2_count = deduped.filter(F.col("cnsmr_phn_id") == "aaa2-0001").count()
        assert a2_count == 3


# ---------------------------------------------------------------------------
# TestDedupSupplfwd
# ---------------------------------------------------------------------------


class TestDedupSupplfwd:
    def test_removes_duplicates_same_key(self, spark, silver_sample_data_dir):
        supplfwd = ingest_supplfwd(spark, str(silver_sample_data_dir))
        original_count = supplfwd.count()
        deduped = dedup_supplfwd(supplfwd)
        # Fixture has 1 duplicate D1 (same legacy_id + phone + record_date) → dedup removes it
        assert deduped.count() == original_count - 1

    def test_preserves_records_with_different_keys(self, spark, silver_sample_data_dir):
        supplfwd = ingest_supplfwd(spark, str(silver_sample_data_dir))
        deduped = dedup_supplfwd(supplfwd)
        # Each unique (legacy_id, phone) pair should still be present after dedup
        distinct_phones = deduped.select("cnsmr_phn_nmbr_txt").distinct().count()
        assert distinct_phones == 4  # B1, C1, D1, G1


# ---------------------------------------------------------------------------
# TestDetectHardDeletes
# ---------------------------------------------------------------------------


class TestDetectHardDeletes:
    def test_phone_missing_from_latest_snapshot(self, spark, silver_sample_data_dir):
        nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
        hard = detect_hard_deletes(dedup_nonfin(nonfin))
        # A1 (5551111111) is in snapshots 1,2 but not 3 → hard deleted
        row = _lookup(hard, {"cnsmr_phn_nmbr_txt": "5551111111"})
        assert row["is_hard_deleted"] is True
        assert row["hard_deleted_at"] == datetime.date(2019, 2, 1)

    def test_phone_in_all_snapshots_not_deleted(self, spark, silver_sample_data_dir):
        nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
        hard = detect_hard_deletes(dedup_nonfin(nonfin))
        # A2 (5552222222) in all snapshots → not hard deleted
        row = _lookup(hard, {"cnsmr_phn_nmbr_txt": "5552222222"})
        assert row["is_hard_deleted"] is False
        assert row["hard_deleted_at"] is None

    def test_phone_in_latest_snapshot_not_deleted(self, spark, silver_sample_data_dir):
        nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
        hard = detect_hard_deletes(dedup_nonfin(nonfin))
        # B1 (5553333333) in all snapshots → not hard deleted
        row = _lookup(hard, {"cnsmr_phn_nmbr_txt": "5553333333"})
        assert row["is_hard_deleted"] is False

    def test_readded_phone_not_hard_deleted(self, spark, silver_sample_data_dir):
        nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
        hard = detect_hard_deletes(dedup_nonfin(nonfin))
        # C1 (5554444444) — absent from snapshot 2 but re-appears in 3
        # last_seen_date = 2019-03-01 = max snapshot date → not hard deleted
        row = _lookup(hard, {"cnsmr_phn_nmbr_txt": "5554444444"})
        assert row["is_hard_deleted"] is False


# ---------------------------------------------------------------------------
# TestDetectSoftDeletes
# ---------------------------------------------------------------------------


class TestDetectSoftDeletes:
    def test_soft_delete_flag_y(self, spark, silver_sample_data_dir):
        supplfwd = ingest_supplfwd(spark, str(silver_sample_data_dir))
        soft = detect_soft_deletes(supplfwd)
        # B1 soft-deleted with flag Y
        row = _lookup(soft, {"phone_number": "5553333333"})
        assert row["is_soft_deleted"] is True
        assert row["soft_deleted_at"] == datetime.date(2019, 4, 1)

    def test_soft_delete_flag_n(self, spark, silver_sample_data_dir):
        supplfwd = ingest_supplfwd(spark, str(silver_sample_data_dir))
        soft = detect_soft_deletes(supplfwd)
        # D1 (5555555555) has sft_dlt_flg='N' → not soft deleted
        row = _lookup(soft, {"phone_number": "5555555555"})
        assert row["is_soft_deleted"] is False

    def test_c1_latest_is_soft_deleted(self, spark, silver_sample_data_dir):
        supplfwd = ingest_supplfwd(spark, str(silver_sample_data_dir))
        soft = detect_soft_deletes(supplfwd)
        # C1 has only one SUPPLFWD record: sft_dlt_flg='Y' at 2019-02-15
        row = _lookup(soft, {"phone_number": "5554444444"})
        assert row["is_soft_deleted"] is True


# ---------------------------------------------------------------------------
# TestMergeNonfinAndSupplfwd
# ---------------------------------------------------------------------------


class TestMergeNonfinAndSupplfwd:
    def _get_merged(self, spark, silver_sample_data_dir):
        nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
        supplfwd = ingest_supplfwd(spark, str(silver_sample_data_dir))
        latest_nf = get_latest_nonfin_state(dedup_nonfin(nonfin))
        latest_sf = get_latest_supplfwd_state(dedup_supplfwd(supplfwd))
        return merge_nonfin_and_supplfwd(latest_nf, latest_sf)

    def test_one_row_per_account_phone(self, spark, silver_sample_data_dir):
        merged = self._get_merged(spark, silver_sample_data_dir)
        total = merged.count()
        distinct = merged.select("legacy_identifier", "phone_number").distinct().count()
        assert total == distinct

    def test_supplfwd_wins_when_more_recent(self, spark, silver_sample_data_dir):
        merged = self._get_merged(spark, silver_sample_data_dir)
        # B1: latest NON-FIN = 2019-03-01, latest SUPPLFWD = 2019-04-01 → SUPPLFWD wins
        row = _lookup(merged, {"phone_number": "5553333333"})
        assert row["record_source"] == "SUPPLFWD"
        assert row["event_date"] == datetime.date(2019, 4, 1)

    def test_nonfin_wins_when_more_recent(self, spark, silver_sample_data_dir):
        merged = self._get_merged(spark, silver_sample_data_dir)
        # C1: latest NON-FIN = 2019-03-01, latest SUPPLFWD = 2019-02-15 → NON-FIN wins
        row = _lookup(merged, {"phone_number": "5554444444"})
        assert row["record_source"] == "NON-FIN"
        assert row["event_date"] == datetime.date(2019, 3, 1)

    def test_phone_only_in_nonfin(self, spark, silver_sample_data_dir):
        merged = self._get_merged(spark, silver_sample_data_dir)
        # A1 (5551111111) — only in NON-FIN, never in SUPPLFWD
        row = _lookup(merged, {"phone_number": "5551111111"})
        assert row["record_source"] == "NON-FIN"

    def test_phone_only_in_supplfwd(self, spark, silver_sample_data_dir):
        merged = self._get_merged(spark, silver_sample_data_dir)
        # D1 (5555555555) — only in SUPPLFWD
        row = _lookup(merged, {"phone_number": "5555555555"})
        assert row["record_source"] == "SUPPLFWD"

    def test_supplfwd_wins_on_date_tie(self, spark, silver_sample_data_dir):
        merged = self._get_merged(spark, silver_sample_data_dir)
        # G1: NON-FIN date=2019-03-01, SUPPLFWD date=2019-03-01 → SUPPLFWD wins tie
        row = _lookup(merged, {"phone_number": "5557777777"})
        assert row["record_source"] == "SUPPLFWD"
        assert row["phone_source"] == "CLIENT"  # SUPPLFWD value; NON-FIN had CONSUMER
        assert row["consent_flag"] == "N"  # SUPPLFWD value; NON-FIN had Y


# ---------------------------------------------------------------------------
# TestBuildSilverTable
# ---------------------------------------------------------------------------


class TestBuildSilverTable:
    def _get_silver(self, spark, silver_sample_data_dir):
        nonfin = ingest_nonfin(spark, str(silver_sample_data_dir))
        supplfwd = ingest_supplfwd(spark, str(silver_sample_data_dir))
        enrl = ingest_enrollment(spark, str(silver_sample_data_dir))
        return build_silver_table(nonfin, supplfwd, enrl)

    def test_output_columns(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        expected = [
            "account_number",
            "legacy_identifier",
            "phone_number",
            "phone_id",
            "phone_type",
            "phone_status",
            "phone_source",
            "phone_technology",
            "quality_score",
            "consent_flag",
            "consent_date",
            "is_deleted",
            "delete_type",
            "deleted_at",
            "record_source",
            "event_date",
        ]
        assert silver.columns == expected

    def test_one_row_per_account_phone(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        total = silver.count()
        distinct = silver.select("account_number", "phone_number").distinct().count()
        assert total == distinct

    def test_hard_delete_detected(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # A1 — hard deleted
        row = _lookup(silver, {"phone_number": "5551111111"})
        assert row["is_deleted"] is True
        assert row["delete_type"] == "hard_delete"
        assert row["deleted_at"] == datetime.date(2019, 2, 1)

    def test_soft_delete_detected(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # B1 — soft deleted via SUPPLFWD after last snapshot
        row = _lookup(silver, {"phone_number": "5553333333"})
        assert row["is_deleted"] is True
        assert row["delete_type"] == "soft_delete"
        assert row["deleted_at"] == datetime.date(2019, 4, 1)

    def test_readd_after_soft_delete(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # C1 — soft-deleted at Feb 15, re-appears in Mar snapshot → not deleted
        row = _lookup(silver, {"phone_number": "5554444444"})
        assert row["is_deleted"] is False
        assert row["delete_type"] is None

    def test_consent_withdrawal(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # A2 — consent changed from Y to N in snapshot 3
        row = _lookup(silver, {"phone_number": "5552222222"})
        assert row["consent_flag"] == "N"
        assert row["consent_date"] == datetime.date(2019, 2, 20)

    def test_null_consent_preserved(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # A1 — never had consent flag set
        row = _lookup(silver, {"phone_number": "5551111111"})
        assert row["consent_flag"] is None or row["consent_flag"] == ""

    def test_account_number_derived(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        row = _lookup(silver, {"phone_number": "5551111111"})
        assert row["account_number"] == "1111111111111111"
        assert row["legacy_identifier"] == "1111111111111111P"

    def test_supplfwd_only_phone(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # D1 — only in SUPPLFWD, active, consent Y
        row = _lookup(silver, {"phone_number": "5555555555"})
        assert row["is_deleted"] is False
        assert row["record_source"] == "SUPPLFWD"
        assert row["consent_flag"] == "Y"
        assert row["account_number"] == "4444444444444444"

    def test_enrollment_only_phone(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # E1 — only in ENRLMT, never in NON-FIN or SUPPLFWD
        row = _lookup(silver, {"phone_number": "5556666666"})
        assert row["account_number"] == "5555555555555555"
        assert row["is_deleted"] is False
        assert row["record_source"] == "ENRLMT"
        assert row["phone_source"] == "CLIENT"

    def test_same_phone_different_accounts(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # C1 (account C, phone 5554444444) — re-added, not deleted
        c1 = _lookup(silver, {"account_number": "3333333333333333", "phone_number": "5554444444"})
        assert c1["is_deleted"] is False
        assert c1["phone_source"] == "CLIENT"
        # F1 (account F, same phone 5554444444) — hard deleted
        f1 = _lookup(silver, {"account_number": "6666666666666666", "phone_number": "5554444444"})
        assert f1["is_deleted"] is True
        assert f1["delete_type"] == "hard_delete"
        assert f1["phone_source"] == "OTHER"

    def test_supplfwd_wins_date_tie(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # G1: both sources have event_date 2019-03-01, SUPPLFWD wins
        row = _lookup(silver, {"phone_number": "5557777777"})
        assert row["record_source"] == "SUPPLFWD"
        assert row["phone_source"] == "CLIENT"
        assert row["consent_flag"] == "N"
        assert row["is_deleted"] is False

    def test_total_row_count(self, spark, silver_sample_data_dir):
        silver = self._get_silver(spark, silver_sample_data_dir)
        # 8 unique (account, phone) pairs: A1, A2, B1, C1, D1, E1, F1, G1
        assert silver.count() == 8
