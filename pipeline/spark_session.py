import os

from pyspark.sql import SparkSession

# PySpark 3.5 requires Java 17 — pin JAVA_HOME if Corretto 17 is available.
_JAVA17_HOME = (
    "/Users/matthewslipenchuk/Library/Java/JavaVirtualMachines"
    "/corretto-17.0.16/Contents/Home"
)
if os.path.isdir(_JAVA17_HOME):
    os.environ["JAVA_HOME"] = _JAVA17_HOME


def get_spark(app_name: str = "phone-compliance", local: bool = True) -> SparkSession:
    """Create a SparkSession configured for local or cluster mode."""
    builder = SparkSession.builder.appName(app_name)

    if local:
        builder = (
            builder.master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
        )

    return builder.getOrCreate()
