"""Run the full Bronze → Silver → Gold pipeline locally and display results."""

import sys

from pipeline.spark_session import get_spark
from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd
from pipeline.silver import build_silver_table
from pipeline.gold import build_phone_consent_table, can_send_sms


def main(data_dir: str) -> None:
    spark = get_spark()

    print("=" * 60)
    print(f"Running Gold layer on: {data_dir}")
    print("=" * 60)

    # Bronze
    print("\nIngesting Bronze data...")
    nonfin = ingest_nonfin(spark, data_dir)
    supplfwd = ingest_supplfwd(spark, data_dir)
    enrl = ingest_enrollment(spark, data_dir)

    # Silver
    print("Building Silver table...")
    silver = build_silver_table(nonfin, supplfwd, enrl)

    # Gold
    print("Building Gold phone_consent table...")
    phone_consent = build_phone_consent_table(silver)
    phone_consent.cache()

    total = phone_consent.count()
    print(f"\nphone_consent table: {total:,} rows")

    print("\n--- SMS consent summary ---")
    phone_consent.groupBy("has_sms_consent").count().show()

    print("--- Phone source summary ---")
    phone_consent.groupBy("phone_source").count().show()

    print("--- Delete summary ---")
    phone_consent.groupBy("is_deleted", "delete_type").count().show()

    print("--- Sample rows ---")
    phone_consent.show(10, truncate=False)

    # Sample compliance checks
    print("--- Sample can_send_sms lookups ---")
    sample_rows = phone_consent.limit(5).collect()
    for row in sample_rows:
        result = can_send_sms(phone_consent, row["account_number"], row["phone_number"])
        print(
            f"  can_send_sms({row['account_number']}, {row['phone_number']}) "
            f"→ {result}  "
            f"[source={row['phone_source']}, deleted={row['is_deleted']}, "
            f"consent={row['has_sms_consent']}]"
        )

    spark.stop()


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "./data_sample"
    main(data_dir)
