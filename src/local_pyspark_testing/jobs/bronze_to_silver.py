from local_pyspark_testing.environment import spark
from local_pyspark_testing.transforms import country_code_to_name


def main():
    df = spark.read.table("bronze.customers")

    df_transformed = df.withColumn(
        "country_name", country_code_to_name(df["country_code"])
    )

    df_transformed.write.mode("overwrite").saveAsTable("silver.customers")


if __name__ == "__main__":
    main()
