from pyspark.sql import DataFrame

from local_pyspark_testing.environment import spark
from local_pyspark_testing.transforms import country_code_to_name


def transform(df: DataFrame) -> DataFrame:
    """Transform bronze customers data to silver layer.

    Enriches customer data with country names derived from country codes.

    Args:
        df: Input DataFrame with at least a 'country_code' column

    Returns:
        Transformed DataFrame with an additional 'country_name' column
    """
    return df.withColumn("country_name", country_code_to_name(df["country_code"]))


def main():
    """Bronze to silver pipeline: read -> transform -> write."""
    df = spark.read.table("bronze.customers")
    df_transformed = transform(df)
    df_transformed.write.mode("overwrite").saveAsTable("silver.customers")


if __name__ == "__main__":
    main()
