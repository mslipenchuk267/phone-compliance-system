"""Cloud entrypoint: Silver layer — read Bronze Delta, write Silver Delta."""

import sys
sys.path.insert(0, "/dbfs/phone-compliance")

from pipeline.data_quality import check_silver_quality, run_checks
from pipeline.delta_io import read_delta, write_delta
from pipeline.silver import build_silver_table
from pipeline.spark_session import get_spark


def main(bronze_path: str, silver_path: str) -> None:
    spark = get_spark()

    print(f"Silver: reading Bronze Delta from {bronze_path}")
    nonfin = read_delta(spark, f"{bronze_path}/nonfin")
    supplfwd = read_delta(spark, f"{bronze_path}/supplfwd")
    enrl = read_delta(spark, f"{bronze_path}/enrollment")

    print("Silver: building reconciled table ...")
    silver = build_silver_table(nonfin, supplfwd, enrl)

    print("Silver: running quality checks ...")
    run_checks(check_silver_quality(silver))

    print(f"Silver: writing Delta to {silver_path}")
    write_delta(silver, f"{silver_path}/phone_state")

    print(f"Silver complete. Rows: {silver.count()}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
