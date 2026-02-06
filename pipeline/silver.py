"""Silver layer: reconcile Bronze NON-FIN, SUPPLFWD, and ENRLMT into a unified phone state."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def derive_account_number(
    df: DataFrame, legacy_col: str = "legacy_identifier"
) -> DataFrame:
    """Derive account_number by removing the trailing character from the legacy identifier."""
    return df.withColumn(
        "account_number",
        F.expr(f"substring({legacy_col}, 1, length({legacy_col}) - 1)"),
    )


def dedup_nonfin(nonfin_df: DataFrame) -> DataFrame:
    """Deduplicate NON-FIN records: one row per (cnsmr_phn_id, _file_date)."""
    window = Window.partitionBy("cnsmr_phn_id", "_file_date").orderBy("_source_file")
    return (
        nonfin_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def dedup_supplfwd(supplfwd_df: DataFrame) -> DataFrame:
    """Deduplicate SUPPLFWD records: one row per (legacy_id, phone_number, record_date)."""
    window = Window.partitionBy(
        "cnsmr_idntfr_lgcy_txt", "cnsmr_phn_nmbr_txt", "record_date"
    ).orderBy("cnsmr_phn_hst_id")
    return (
        supplfwd_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def detect_hard_deletes(nonfin_df: DataFrame) -> DataFrame:
    """Detect hard deletes: phones whose last NON-FIN appearance is before the latest snapshot.

    Returns one row per (cnsmr_idntfr_lgcy_txt, cnsmr_phn_nmbr_txt) with:
      - last_seen_date: the last snapshot the phone appeared in
      - is_hard_deleted: True if last_seen_date < global latest snapshot date
      - hard_deleted_at: last_seen_date if hard deleted, else None
    """
    max_snapshot_date = nonfin_df.agg(F.max("_file_date")).collect()[0][0]

    phone_last_seen = nonfin_df.groupBy(
        "cnsmr_idntfr_lgcy_txt", "cnsmr_phn_nmbr_txt"
    ).agg(F.max("_file_date").alias("last_seen_date"))

    return phone_last_seen.withColumn(
        "is_hard_deleted", F.col("last_seen_date") < F.lit(max_snapshot_date)
    ).withColumn(
        "hard_deleted_at",
        F.when(F.col("is_hard_deleted"), F.col("last_seen_date")),
    )


def get_latest_nonfin_state(nonfin_df: DataFrame) -> DataFrame:
    """Get the latest NON-FIN record per (legacy_id, phone_number)."""
    window = Window.partitionBy(
        "cnsmr_idntfr_lgcy_txt", "cnsmr_phn_nmbr_txt"
    ).orderBy(F.col("_file_date").desc())
    return (
        nonfin_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def get_latest_supplfwd_state(supplfwd_df: DataFrame) -> DataFrame:
    """Get the latest SUPPLFWD record per (legacy_id, phone_number)."""
    df = supplfwd_df.withColumn(
        "record_date_parsed", F.to_date("record_date", "yyyy-MM-dd")
    )
    window = Window.partitionBy(
        "cnsmr_idntfr_lgcy_txt", "cnsmr_phn_nmbr_txt"
    ).orderBy(F.col("record_date_parsed").desc(), F.col("cnsmr_phn_hst_id").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def detect_soft_deletes(supplfwd_df: DataFrame) -> DataFrame:
    """Get the latest soft-delete status per (legacy_id, phone_number) from SUPPLFWD."""
    latest = get_latest_supplfwd_state(supplfwd_df)
    return latest.select(
        F.col("cnsmr_idntfr_lgcy_txt").alias("legacy_identifier"),
        F.col("cnsmr_phn_nmbr_txt").alias("phone_number"),
        F.col("cnsmr_phn_sft_dlt_flg").alias("soft_delete_flag"),
        F.col("record_date_parsed").alias("soft_deleted_at"),
    ).withColumn("is_soft_deleted", F.col("soft_delete_flag") == "Y")


def merge_nonfin_and_supplfwd(
    latest_nonfin: DataFrame,
    latest_supplfwd: DataFrame,
) -> DataFrame:
    """Merge latest NON-FIN and SUPPLFWD records, preferring the more recent one.

    Returns one row per (legacy_identifier, phone_number) with reconciled attributes.
    On date tie, SUPPLFWD wins (more authoritative).
    """
    nf = latest_nonfin.select(
        F.col("cnsmr_idntfr_lgcy_txt").alias("legacy_identifier"),
        F.col("cnsmr_phn_nmbr_txt").alias("phone_number"),
        F.col("cnsmr_phn_id").alias("phone_id"),
        F.col("cnsmr_phn_typ_val_txt").alias("phone_type"),
        F.col("cnsmr_phn_stts_val_txt").alias("phone_status"),
        F.col("cnsmr_phn_src_val_txt").alias("phone_source"),
        F.col("cnsmr_phn_tchnlgy_typ_val_txt").alias("phone_technology"),
        F.col("cnsmr_phn_qlty_score_nmbr").alias("quality_score"),
        F.col("cnsmr_phn_cnsnt_flg").alias("consent_flag"),
        F.to_date("cnsmr_phn_cnsnt_dt", "yyyy-MM-dd").alias("consent_date"),
        F.col("_file_date").alias("event_date"),
        F.lit(None).cast("string").alias("soft_delete_flag"),
        F.lit("NON-FIN").alias("record_source"),
        F.col("_source_file"),
    )

    sf = latest_supplfwd.select(
        F.col("cnsmr_idntfr_lgcy_txt").alias("legacy_identifier"),
        F.col("cnsmr_phn_nmbr_txt").alias("phone_number"),
        F.lit(None).cast("string").alias("phone_id"),
        F.col("cnsmr_phn_typ_val_txt").alias("phone_type"),
        F.col("cnsmr_phn_stts_val_txt").alias("phone_status"),
        F.col("cnsmr_phn_src_val_txt").alias("phone_source"),
        F.col("cnsmr_phn_tchnlgy_typ_val_txt").alias("phone_technology"),
        F.col("cnsmr_phn_qlty_score_nmbr").alias("quality_score"),
        F.col("cnsmr_phn_cnsnt_flg").alias("consent_flag"),
        F.to_date("cnsmr_phn_cnsnt_dt", "yyyy-MM-dd").alias("consent_date"),
        F.col("record_date_parsed").alias("event_date"),
        F.col("cnsmr_phn_sft_dlt_flg").alias("soft_delete_flag"),
        F.lit("SUPPLFWD").alias("record_source"),
        F.col("_source_file"),
    )

    combined = nf.unionByName(sf)
    window = Window.partitionBy("legacy_identifier", "phone_number").orderBy(
        F.col("event_date").desc(),
        F.when(F.col("record_source") == "SUPPLFWD", 0).otherwise(1),
    )
    return (
        combined.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _normalize_enrollment(enrollment_df: DataFrame) -> DataFrame:
    """Normalize enrollment records into the common merge schema."""
    return enrollment_df.select(
        F.col("consumer_legacy_identifier").alias("legacy_identifier"),
        F.col("phone_number"),
        F.lit(None).cast("string").alias("phone_id"),
        F.col("phone_type"),
        F.col("phone_status"),
        F.col("phone_source"),
        F.col("phone_technology"),
        F.col("quality_score"),
        F.lit(None).cast("string").alias("consent_flag"),
        F.lit(None).cast("date").alias("consent_date"),
        F.to_date("enrollment_date", "yyyy-MM-dd").alias("event_date"),
        F.lit(None).cast("string").alias("soft_delete_flag"),
        F.lit("ENRLMT").alias("record_source"),
        F.col("_source_file"),
    )


def build_silver_table(
    nonfin_df: DataFrame,
    supplfwd_df: DataFrame,
    enrollment_df: DataFrame,
) -> DataFrame:
    """Build the Silver phone state table from Bronze DataFrames.

    Returns one row per (account_number, phone_number) with reconciled state.
    """
    # Step 1: Dedup
    nonfin_deduped = dedup_nonfin(nonfin_df)
    supplfwd_deduped = dedup_supplfwd(supplfwd_df)

    # Step 2: Hard deletes from NON-FIN
    hard_deletes = detect_hard_deletes(nonfin_deduped)

    # Step 3: Soft deletes from SUPPLFWD
    soft_deletes = detect_soft_deletes(supplfwd_deduped)

    # Step 4: Latest state from each source
    latest_nonfin = get_latest_nonfin_state(nonfin_deduped)
    latest_supplfwd = get_latest_supplfwd_state(supplfwd_deduped)

    # Step 5: Merge NON-FIN and SUPPLFWD into unified record
    merged = merge_nonfin_and_supplfwd(latest_nonfin, latest_supplfwd)

    # Step 5b: Include enrollment-only phones (not already in NON-FIN or SUPPLFWD)
    enrl_normalized = _normalize_enrollment(enrollment_df)
    enrl_window = Window.partitionBy("legacy_identifier", "phone_number").orderBy(
        F.col("event_date").desc()
    )
    latest_enrl = (
        enrl_normalized.withColumn("_rn", F.row_number().over(enrl_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    enrl_only = latest_enrl.join(
        merged.select("legacy_identifier", "phone_number"),
        on=["legacy_identifier", "phone_number"],
        how="left_anti",
    )
    merged = merged.unionByName(enrl_only)

    # Step 6: Join hard-delete info
    merged = merged.join(
        hard_deletes.select(
            F.col("cnsmr_idntfr_lgcy_txt").alias("legacy_identifier"),
            F.col("cnsmr_phn_nmbr_txt").alias("phone_number"),
            "is_hard_deleted",
            "hard_deleted_at",
        ),
        on=["legacy_identifier", "phone_number"],
        how="left",
    )

    # Step 7: Join soft-delete info
    merged = merged.join(
        soft_deletes.select(
            "legacy_identifier",
            "phone_number",
            "is_soft_deleted",
            "soft_deleted_at",
        ),
        on=["legacy_identifier", "phone_number"],
        how="left",
    )

    # Step 8: Compute unified delete status
    # Re-add: if winning record is NON-FIN and event_date > soft_deleted_at, soft delete is overridden
    result = (
        merged.withColumn(
            "is_soft_deleted_final",
            F.when(
                F.col("is_soft_deleted")
                & (F.col("record_source") == "NON-FIN")
                & (F.col("event_date") > F.col("soft_deleted_at")),
                F.lit(False),
            ).otherwise(F.coalesce(F.col("is_soft_deleted"), F.lit(False))),
        )
        .withColumn(
            "is_deleted",
            F.coalesce(F.col("is_hard_deleted"), F.lit(False))
            | F.col("is_soft_deleted_final"),
        )
        .withColumn(
            "delete_type",
            F.when(F.col("is_hard_deleted"), F.lit("hard_delete"))
            .when(F.col("is_soft_deleted_final"), F.lit("soft_delete"))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "deleted_at",
            F.when(F.col("is_hard_deleted"), F.col("hard_deleted_at"))
            .when(F.col("is_soft_deleted_final"), F.col("soft_deleted_at"))
            .otherwise(F.lit(None)),
        )
    )

    # Step 9: Derive account_number and select final columns
    result = derive_account_number(result)

    return result.select(
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
        "_source_file",
    )
