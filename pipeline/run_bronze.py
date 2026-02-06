"""Run the Bronze layer pipeline locally and display results."""

import sys

from pipeline.spark_session import get_spark
from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd


def main(data_dir: str) -> None:
    spark = get_spark()

    print("=" * 60)
    print(f"Reading sample data from: {data_dir}")
    print("=" * 60)

    # Enrollment
    enrl = ingest_enrollment(spark, data_dir)
    print(f"\n--- ENROLLMENT ---")
    print(f"Records: {enrl.count()}")
    print(f"Columns: {enrl.columns}")
    enrl.show(3, truncate=False)

    # NON-FIN-BDL
    nonfin = ingest_nonfin(spark, data_dir)
    print(f"\n--- NON-FIN-BDL ---")
    print(f"Records: {nonfin.count()}")
    print(f"Columns: {nonfin.columns}")
    nonfin.show(3, truncate=False)

    # SUPPLFWD
    suppl = ingest_supplfwd(spark, data_dir)
    print(f"\n--- SUPPLFWD ---")
    print(f"Records: {suppl.count()}")
    print(f"Columns: {suppl.columns}")
    suppl.show(3, truncate=False)

    print("\n" + "=" * 60)
    print("Smoke test passed!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "./data_sample"
    main(data_dir)
