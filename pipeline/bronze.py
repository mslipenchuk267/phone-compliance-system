"""Bronze layer ingestion: read raw gzipped pipe-delimited files into Spark DataFrames."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, to_date


def _read_pipe_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a gzipped pipe-delimited CSV directory/file into a DataFrame."""
    return spark.read.csv(
        path,
        sep="|",
        header=True,
        inferSchema=False,
    )


def _extract_file_date(df: DataFrame) -> DataFrame:
    """Extract the YYYYMMDD date from the filename and add as a date column.

    Filenames look like: ...-20190101000000863-...
    The first 8 digits after the dash are the date.
    """
    return df.withColumn("_source_file", input_file_name()).withColumn(
        "_file_date",
        to_date(regexp_extract("_source_file", r"-(\d{8})\d+-", 1), "yyyyMMdd"),
    )


def ingest_enrollment(spark: SparkSession, data_dir: str) -> DataFrame:
    """Ingest ENRLMT files from the ENROLLMENT directory."""
    path = str(Path(data_dir) / "ENROLLMENT" / "*ENRLMT*.csv.gz")
    df = _read_pipe_csv(spark, path)
    return _extract_file_date(df)


def ingest_nonfin(spark: SparkSession, data_dir: str) -> DataFrame:
    """Ingest NON-FIN-BDL files from the MAINTENANCE directory."""
    path = str(Path(data_dir) / "MAINTENANCE" / "*NON-FIN-BDL*.csv.gz")
    df = _read_pipe_csv(spark, path)
    return _extract_file_date(df)


def ingest_supplfwd(spark: SparkSession, data_dir: str) -> DataFrame:
    """Ingest SUPPLFWD files from the MAINTENANCE directory."""
    path = str(Path(data_dir) / "MAINTENANCE" / "*SUPPLFWD*.csv.gz")
    df = _read_pipe_csv(spark, path)
    return _extract_file_date(df)
