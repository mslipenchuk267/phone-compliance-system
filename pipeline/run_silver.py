"""Run the Silver layer pipeline locally and display results."""

import sys

from pipeline.spark_session import get_spark
from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd
from pipeline.silver import build_silver_table


def main(data_dir: str) -> None:
    spark = get_spark()

    print("=" * 60)
    print(f"Running Silver layer on: {data_dir}")
    print("=" * 60)

    # Bronze ingestion
    print("\nIngesting Bronze data...")
    nonfin = ingest_nonfin(spark, data_dir)
    supplfwd = ingest_supplfwd(spark, data_dir)
    enrl = ingest_enrollment(spark, data_dir)
    print(f"  NON-FIN:    {nonfin.count():>10,} rows")
    print(f"  SUPPLFWD:   {supplfwd.count():>10,} rows")
    print(f"  ENROLLMENT: {enrl.count():>10,} rows")

    # Silver processing
    print("\nBuilding Silver table...")
    silver = build_silver_table(nonfin, supplfwd, enrl)
    silver.cache()

    total = silver.count()
    print(f"\nSilver table: {total:,} rows")

    # Summary stats
    print("\n--- Delete summary ---")
    silver.groupBy("delete_type").count().show()

    print("--- Consent summary ---")
    silver.groupBy("consent_flag").count().show()

    print("--- Phone source summary ---")
    silver.groupBy("phone_source").count().show()

    print("--- Record source (winning) ---")
    silver.groupBy("record_source").count().show()

    print("--- Sample rows ---")
    silver.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "./data_sample"
    main(data_dir)
