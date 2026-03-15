import os

from pyspark.sql import SparkSession


def _is_local() -> bool:
    return os.environ.get("LOCAL_SPARK") == "true"


def _create_local_spark() -> SparkSession:
    return (
        SparkSession.builder
        # Single thread - avoids thread pool overhead for small test data
        .master("local[1]")
        .appName("local-testing")
        # Single partition for shuffles - default 200 is overkill for test data
        .config("spark.sql.shuffle.partitions", "1")
        # Single partition for RDD ops - matches our single-threaded setup
        .config("spark.default.parallelism", "1")
        # Disable web UI - saves memory and startup time
        .config("spark.ui.enabled", "false")
        # Disable adaptive query execution - adds planning overhead not worth it for small data
        .config("spark.sql.adaptive.enabled", "false")
        # Local warehouse dir - avoids metastore setup
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )


def _create_databricks_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_spark() -> SparkSession:
    if _is_local():
        return _create_local_spark()
    return _create_databricks_spark()


spark = get_spark()
