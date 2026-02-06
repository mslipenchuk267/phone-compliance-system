"""Cloud entrypoint: Gold layer — read Silver Delta, write Gold Delta."""

import sys
sys.path.insert(0, "/dbfs/phone-compliance")

from pipeline.data_quality import check_gold_quality, run_checks
from pipeline.delta_io import merge_delta, read_delta
from pipeline.gold import build_phone_consent_table
from pipeline.spark_session import get_spark


def main(silver_path: str, gold_path: str) -> None:
    spark = get_spark()

    print(f"Gold: reading Silver Delta from {silver_path}")
    silver = read_delta(spark, f"{silver_path}/phone_state")

    print("Gold: building phone_consent table ...")
    gold = build_phone_consent_table(silver)

    print("Gold: running quality checks ...")
    run_checks(check_gold_quality(gold))

    print(f"Gold: writing Delta to {gold_path} (merge by account + phone)")
    merge_delta(spark, gold, f"{gold_path}/phone_consent",
                merge_keys=["account_number", "phone_number"])

    print(f"Gold complete. Rows: {gold.count()}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
