"""Data quality checks for each pipeline layer."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F


@dataclass
class QualityCheckResult:
    """Result of a single quality check."""

    name: str
    passed: bool
    details: str = ""

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}: {self.details}"


def run_checks(results: list[QualityCheckResult]) -> bool:
    """Log all check results and return True if all passed."""
    all_passed = True
    for r in results:
        print(r)
        if not r.passed:
            all_passed = False
    return all_passed


def check_bronze_quality(df: DataFrame, source: str) -> list[QualityCheckResult]:
    """Run quality checks on a Bronze DataFrame."""
    results = []

    count = df.count()
    results.append(QualityCheckResult(
        f"{source}_row_count_positive",
        count > 0,
        f"{count} rows",
    ))

    null_dates = df.filter(F.col("_file_date").isNull()).count()
    results.append(QualityCheckResult(
        f"{source}_no_null_file_dates",
        null_dates == 0,
        f"{null_dates} null _file_date rows",
    ))

    return results


def check_silver_quality(df: DataFrame) -> list[QualityCheckResult]:
    """Run quality checks on the Silver DataFrame."""
    results = []

    total = df.count()
    distinct = df.select("account_number", "phone_number").distinct().count()
    results.append(QualityCheckResult(
        "silver_no_duplicate_account_phone",
        total == distinct,
        f"total={total}, distinct={distinct}",
    ))

    nulls = df.filter(F.col("account_number").isNull()).count()
    results.append(QualityCheckResult(
        "silver_no_null_accounts",
        nulls == 0,
        f"{nulls} null account_numbers",
    ))

    return results


def check_gold_quality(df: DataFrame) -> list[QualityCheckResult]:
    """Run quality checks on the Gold DataFrame."""
    results = []

    bad_phones = df.filter(~F.col("phone_number").rlike(r"^\+1\d{10}$")).count()
    results.append(QualityCheckResult(
        "gold_phones_normalized",
        bad_phones == 0,
        f"{bad_phones} non-normalized phone numbers",
    ))

    sources = {r.phone_source for r in df.select("phone_source").distinct().collect()}
    valid = sources.issubset({"CLIENT", "CONSUMER", "THIRD_PARTY", "OTHER"})
    results.append(QualityCheckResult(
        "gold_valid_phone_sources",
        valid,
        f"sources: {sources}",
    ))

    delete_types = {
        r.delete_type for r in df.select("delete_type").distinct().collect()
    }
    valid_types = delete_types.issubset({"hard_delete", "soft_delete", None})
    results.append(QualityCheckResult(
        "gold_valid_delete_types",
        valid_types,
        f"delete_types: {delete_types}",
    ))

    return results
