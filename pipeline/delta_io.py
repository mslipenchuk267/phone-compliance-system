"""Delta Lake I/O utilities for cloud pipeline runs."""

from pyspark.sql import DataFrame, SparkSession


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read a Delta table from the given path."""
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write a DataFrame as a Delta table."""
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)


def merge_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: list[str],
) -> None:
    """MERGE source DataFrame into an existing Delta table (upsert).

    If the target table does not exist yet, creates it via a full write.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, target_path):
        write_delta(source_df, target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    (
        target.alias("target")
        .merge(source_df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
