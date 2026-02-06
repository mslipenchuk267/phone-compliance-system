"""Cloud entrypoint: Bronze layer — read raw CSVs from S3, write Delta tables."""

import sys
sys.path.insert(0, "/dbfs/phone-compliance")

from pipeline.bronze import ingest_enrollment, ingest_nonfin, ingest_supplfwd
from pipeline.data_quality import check_bronze_quality, run_checks
from pipeline.delta_io import write_delta
from pipeline.spark_session import get_spark


def main(data_dir: str, delta_path: str) -> None:
    spark = get_spark()

    print(f"Bronze: reading raw data from {data_dir}")
    enrl = ingest_enrollment(spark, data_dir)
    nonfin = ingest_nonfin(spark, data_dir)
    supplfwd = ingest_supplfwd(spark, data_dir)

    print("Bronze: running quality checks ...")
    checks = (
        check_bronze_quality(enrl, "enrollment")
        + check_bronze_quality(nonfin, "nonfin")
        + check_bronze_quality(supplfwd, "supplfwd")
    )
    run_checks(checks)

    print(f"Bronze: writing Delta tables to {delta_path}")
    write_delta(enrl, f"{delta_path}/enrollment")
    write_delta(nonfin, f"{delta_path}/nonfin")
    write_delta(supplfwd, f"{delta_path}/supplfwd")

    print(
        f"Bronze complete. Rows: enrl={enrl.count()}, "
        f"nonfin={nonfin.count()}, supplfwd={supplfwd.count()}"
    )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
