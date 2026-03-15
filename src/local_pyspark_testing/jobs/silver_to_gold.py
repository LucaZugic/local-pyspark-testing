from pyspark.sql import functions as F

from local_pyspark_testing.environment import spark


def main():
    df = spark.read.table("silver.customers")

    df_aggregated = df.groupBy("country_name").agg(
        F.count("*").alias("customer_count")
    )

    df_aggregated.write.mode("overwrite").saveAsTable("gold.customers_by_country")


if __name__ == "__main__":
    main()
