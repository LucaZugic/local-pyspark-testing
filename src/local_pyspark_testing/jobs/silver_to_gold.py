from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from local_pyspark_testing.environment import spark


def transform(df: DataFrame) -> DataFrame:
    """Transform silver customers data to gold layer.

    Aggregates customer data by country to produce summary statistics.

    Args:
        df: Input DataFrame with at least a 'country_name' column

    Returns:
        Aggregated DataFrame with country_name and customer_count columns
    """
    return df.groupBy("country_name").agg(F.count("*").alias("customer_count"))


def main():
    """Silver to gold pipeline: read -> transform -> write."""
    df = spark.read.table("silver.customers")
    df_aggregated = transform(df)
    df_aggregated.write.mode("overwrite").saveAsTable("gold.customers_by_country")


if __name__ == "__main__":
    main()
