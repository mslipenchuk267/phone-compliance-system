"""Gold layer: build the phone_consent table and can_send_sms compliance API."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_phone_number(df: DataFrame) -> DataFrame:
    """Normalize phone_number to +1XXXXXXXXXX format by prepending '+1'."""
    return df.withColumn("phone_number", F.concat(F.lit("+1"), F.col("phone_number")))


def derive_sms_consent(df: DataFrame) -> DataFrame:
    """Convert consent_flag ('Y'/'N'/NULL/empty) to has_sms_consent (boolean/NULL)."""
    return df.withColumn(
        "has_sms_consent",
        F.when(F.col("consent_flag") == "Y", F.lit(True))
        .when(F.col("consent_flag") == "N", F.lit(False))
        .otherwise(F.lit(None).cast("boolean")),
    )


def build_phone_consent_table(silver_df: DataFrame) -> DataFrame:
    """Build the Gold phone_consent table from the Silver output.

    Returns one row per (account_number, phone_number) with the final schema.
    """
    df = normalize_phone_number(silver_df)
    df = derive_sms_consent(df)

    return df.select(
        "account_number",
        "phone_number",
        "phone_source",
        "has_sms_consent",
        F.col("consent_date").alias("sms_consent_updated_at"),
        "is_deleted",
        "delete_type",
        "deleted_at",
    )


def can_send_sms(
    phone_consent_df: DataFrame, account_number: str, phone_number: str
) -> bool:
    """Check whether SMS outreach is permitted for a given account and phone.

    Returns True only if:
      - The (account_number, phone_number) row exists
      - phone_source is 'CLIENT'
      - is_deleted is False
      - has_sms_consent is True or NULL (implicit consent allowed)
    """
    row = (
        phone_consent_df.filter(
            (F.col("account_number") == account_number)
            & (F.col("phone_number") == phone_number)
        )
        .select("phone_source", "is_deleted", "has_sms_consent")
        .collect()
    )

    if not row:
        return False

    r = row[0]
    return (
        r["phone_source"] == "CLIENT"
        and r["is_deleted"] is False
        and r["has_sms_consent"] is not False
    )
