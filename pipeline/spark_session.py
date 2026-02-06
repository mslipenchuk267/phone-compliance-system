"""SparkSession factory: auto-detects Databricks runtime vs local development."""

import os

from pyspark.sql import SparkSession

# PySpark 3.5 requires Java 17 — pin JAVA_HOME if Corretto 17 is available.
_JAVA17_HOME = (
    "/Users/matthewslipenchuk/Library/Java/JavaVirtualMachines"
    "/corretto-17.0.16/Contents/Home"
)


def _is_databricks() -> bool:
    """Detect if running inside a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "phone-compliance", local: bool | None = None) -> SparkSession:
    """Create a SparkSession configured for local or cluster mode.

    Args:
        app_name: Spark application name.
        local: Force local mode. If None, auto-detect (local unless on Databricks).
    """
    if local is None:
        local = not _is_databricks()

    builder = SparkSession.builder.appName(app_name)

    if local:
        if os.path.isdir(_JAVA17_HOME):
            os.environ["JAVA_HOME"] = _JAVA17_HOME

        builder = (
            builder.master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
        )
    else:
        # Databricks / cluster mode: Delta extensions (belt-and-suspenders,
        # also set via Terraform spark_conf on the job cluster).
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

    return builder.getOrCreate()
