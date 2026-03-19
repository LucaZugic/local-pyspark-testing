from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from local_pyspark_testing.environment import spark
from local_pyspark_testing.jobs import bronze_to_silver, silver_to_gold


@pytest.fixture(scope="module")
def spark_session():
    yield spark


class TestBronzeToSilverTransform:
    """Unit tests for bronze_to_silver.transform function."""

    def test_adds_country_name_column(self, spark_session):
        """Test that country_name column is added with correct values."""
        df_input = spark_session.createDataFrame(
            [
                (1, "John", "GB"),
                (2, "Anna", "DE"),
                (3, "Bob", "US"),
            ],
            ["id", "name", "country_code"],
        )
        df_expected = spark_session.createDataFrame(
            [
                (1, "John", "GB", "United Kingdom"),
                (2, "Anna", "DE", "Germany"),
                (3, "Bob", "US", "United States"),
            ],
            ["id", "name", "country_code", "country_name"],
        )

        df_result = bronze_to_silver.transform(df_input)

        assertDataFrameEqual(df_result, df_expected)

    def test_handles_null_country_codes(self, spark_session):
        """Test that null country codes result in null country names."""
        schema_in = StructType(
            [
                StructField("id", LongType()),
                StructField("country_code", StringType()),
            ]
        )
        df_input = spark_session.createDataFrame(
            [(1, None), (2, "GB")],
            schema_in,
        )
        schema_out = StructType(
            [
                StructField("id", LongType()),
                StructField("country_code", StringType()),
                StructField("country_name", StringType()),
            ]
        )
        df_expected = spark_session.createDataFrame(
            [(1, None, None), (2, "GB", "United Kingdom")],
            schema_out,
        )

        df_result = bronze_to_silver.transform(df_input)

        assertDataFrameEqual(df_result, df_expected)

    def test_handles_invalid_country_codes(self, spark_session):
        """Test that invalid country codes result in null country names."""
        df_input = spark_session.createDataFrame(
            [(1, "XX"), (2, "YY"), (3, "GB")],
            ["id", "country_code"],
        )
        schema_out = StructType(
            [
                StructField("id", LongType()),
                StructField("country_code", StringType()),
                StructField("country_name", StringType()),
            ]
        )
        df_expected = spark_session.createDataFrame(
            [(1, "XX", None), (2, "YY", None), (3, "GB", "United Kingdom")],
            schema_out,
        )

        df_result = bronze_to_silver.transform(df_input)

        assertDataFrameEqual(df_result, df_expected)

    def test_handles_lowercase_country_codes(self, spark_session):
        """Test that lowercase country codes are handled correctly."""
        df_input = spark_session.createDataFrame(
            [(1, "gb"), (2, "de")],
            ["id", "country_code"],
        )
        df_expected = spark_session.createDataFrame(
            [(1, "gb", "United Kingdom"), (2, "de", "Germany")],
            ["id", "country_code", "country_name"],
        )

        df_result = bronze_to_silver.transform(df_input)

        assertDataFrameEqual(df_result, df_expected)

    def test_handles_empty_dataframe(self, spark_session):
        """Test that empty DataFrame is handled correctly."""
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("country_code", StringType()),
            ]
        )
        df_input = spark_session.createDataFrame([], schema)
        schema_out = StructType(
            [
                StructField("id", LongType()),
                StructField("country_code", StringType()),
                StructField("country_name", StringType()),
            ]
        )
        df_expected = spark_session.createDataFrame([], schema_out)

        df_result = bronze_to_silver.transform(df_input)

        assertDataFrameEqual(df_result, df_expected)

    def test_preserves_all_original_columns(self, spark_session):
        """Test that all original columns are preserved."""
        df_input = spark_session.createDataFrame(
            [(1, "John", "john@example.com", "GB", "2023-01-01")],
            ["id", "name", "email", "country_code", "created_at"],
        )

        df_result = bronze_to_silver.transform(df_input)

        assert set(df_result.columns) == {
            "id",
            "name",
            "email",
            "country_code",
            "created_at",
            "country_name",
        }


class TestBronzeToSilverMain:
    """End-to-end tests for bronze_to_silver.main function."""

    @patch("local_pyspark_testing.jobs.bronze_to_silver.transform")
    @patch("local_pyspark_testing.jobs.bronze_to_silver.spark")
    def test_reads_from_bronze_customers(self, mock_spark, mock_transform):
        """Test that main reads from correct bronze table."""
        mock_df = MagicMock()
        mock_spark.read.table.return_value = mock_df
        mock_transform.return_value = mock_df

        bronze_to_silver.main()

        mock_spark.read.table.assert_called_once_with("bronze.customers")

    @patch("local_pyspark_testing.jobs.bronze_to_silver.transform")
    @patch("local_pyspark_testing.jobs.bronze_to_silver.spark")
    def test_writes_to_silver_customers(self, mock_spark, mock_transform):
        """Test that main writes to correct silver table."""
        mock_df = MagicMock()
        mock_spark.read.table.return_value = mock_df
        mock_transform.return_value = mock_df

        bronze_to_silver.main()

        mock_df.write.mode.assert_called_once_with("overwrite")
        mock_df.write.mode.return_value.saveAsTable.assert_called_once_with(
            "silver.customers"
        )

    @patch("local_pyspark_testing.jobs.bronze_to_silver.transform")
    @patch("local_pyspark_testing.jobs.bronze_to_silver.spark")
    def test_applies_transformation(self, mock_spark, mock_transform):
        """Test that transformation is applied correctly."""
        mock_input_df = MagicMock()
        mock_output_df = MagicMock()
        mock_spark.read.table.return_value = mock_input_df
        mock_transform.return_value = mock_output_df

        bronze_to_silver.main()

        mock_transform.assert_called_once_with(mock_input_df)
        mock_output_df.write.mode.return_value.saveAsTable.assert_called_once()


class TestSilverToGoldTransform:
    """Unit tests for silver_to_gold.transform function."""

    def test_aggregates_by_country_name(self, spark_session):
        """Test that data is correctly aggregated by country_name."""
        df_input = spark_session.createDataFrame(
            [
                (1, "John", "United Kingdom"),
                (2, "Anna", "Germany"),
                (3, "Bob", "United Kingdom"),
                (4, "Carl", "Germany"),
                (5, "Diana", "United States"),
            ],
            ["id", "name", "country_name"],
        )
        df_expected = spark_session.createDataFrame(
            [
                ("Germany", 2),
                ("United Kingdom", 2),
                ("United States", 1),
            ],
            ["country_name", "customer_count"],
        )

        df_result = silver_to_gold.transform(df_input)

        assertDataFrameEqual(df_result, df_expected, checkRowOrder=False)

    def test_handles_single_country(self, spark_session):
        """Test aggregation with only one country."""
        df_input = spark_session.createDataFrame(
            [(1, "United Kingdom"), (2, "United Kingdom"), (3, "United Kingdom")],
            ["id", "country_name"],
        )
        df_expected = spark_session.createDataFrame(
            [("United Kingdom", 3)],
            ["country_name", "customer_count"],
        )

        df_result = silver_to_gold.transform(df_input)

        assertDataFrameEqual(df_result, df_expected)

    def test_handles_null_country_names(self, spark_session):
        """Test that null country names are grouped separately."""
        schema_in = StructType(
            [
                StructField("id", LongType()),
                StructField("country_name", StringType()),
            ]
        )
        df_input = spark_session.createDataFrame(
            [(1, None), (2, None), (3, "Germany"), (4, "Germany")],
            schema_in,
        )
        schema_out = StructType(
            [
                StructField("country_name", StringType()),
                StructField("customer_count", LongType()),
            ]
        )
        df_expected = spark_session.createDataFrame(
            [(None, 2), ("Germany", 2)],
            schema_out,
        )

        df_result = silver_to_gold.transform(df_input)

        assertDataFrameEqual(df_result, df_expected, checkRowOrder=False)

    def test_handles_empty_dataframe(self, spark_session):
        """Test that empty DataFrame is handled correctly."""
        schema_in = StructType(
            [
                StructField("id", LongType()),
                StructField("country_name", StringType()),
            ]
        )
        df_input = spark_session.createDataFrame([], schema_in)
        schema_out = StructType(
            [
                StructField("country_name", StringType()),
                StructField("customer_count", LongType()),
            ]
        )
        df_expected = spark_session.createDataFrame([], schema_out)

        df_result = silver_to_gold.transform(df_input)

        assertDataFrameEqual(df_result, df_expected)

    def test_counts_all_rows_for_country(self, spark_session):
        """Test that customer_count accurately reflects row counts."""
        df_input = spark_session.createDataFrame(
            [(i, "Germany") for i in range(1, 101)],
            ["id", "country_name"],
        )
        df_expected = spark_session.createDataFrame(
            [("Germany", 100)],
            ["country_name", "customer_count"],
        )

        df_result = silver_to_gold.transform(df_input)

        assertDataFrameEqual(df_result, df_expected)

    def test_output_has_correct_columns(self, spark_session):
        """Test that output DataFrame has exactly the expected columns."""
        df_input = spark_session.createDataFrame(
            [(1, "John", "United Kingdom", "extra_data")],
            ["id", "name", "country_name", "extra_column"],
        )

        df_result = silver_to_gold.transform(df_input)

        assert set(df_result.columns) == {"country_name", "customer_count"}


class TestSilverToGoldMain:
    """End-to-end tests for silver_to_gold.main function."""

    @patch("local_pyspark_testing.jobs.silver_to_gold.transform")
    @patch("local_pyspark_testing.jobs.silver_to_gold.spark")
    def test_reads_from_silver_customers(self, mock_spark, mock_transform):
        """Test that main reads from correct silver table."""
        mock_df = MagicMock()
        mock_spark.read.table.return_value = mock_df
        mock_transform.return_value = mock_df

        silver_to_gold.main()

        mock_spark.read.table.assert_called_once_with("silver.customers")

    @patch("local_pyspark_testing.jobs.silver_to_gold.transform")
    @patch("local_pyspark_testing.jobs.silver_to_gold.spark")
    def test_writes_to_gold_customers_by_country(self, mock_spark, mock_transform):
        """Test that main writes to correct gold table."""
        mock_df = MagicMock()
        mock_spark.read.table.return_value = mock_df
        mock_transform.return_value = mock_df

        silver_to_gold.main()

        mock_df.write.mode.assert_called_once_with("overwrite")
        mock_df.write.mode.return_value.saveAsTable.assert_called_once_with(
            "gold.customers_by_country"
        )

    @patch("local_pyspark_testing.jobs.silver_to_gold.transform")
    @patch("local_pyspark_testing.jobs.silver_to_gold.spark")
    def test_applies_aggregation(self, mock_spark, mock_transform):
        """Test that aggregation is applied correctly."""
        mock_input_df = MagicMock()
        mock_output_df = MagicMock()
        mock_spark.read.table.return_value = mock_input_df
        mock_transform.return_value = mock_output_df

        silver_to_gold.main()

        mock_transform.assert_called_once_with(mock_input_df)
        mock_output_df.write.mode.return_value.saveAsTable.assert_called_once()

    @patch("local_pyspark_testing.jobs.silver_to_gold.transform")
    @patch("local_pyspark_testing.jobs.silver_to_gold.spark")
    def test_pipeline_flow(self, mock_spark, mock_transform):
        """Test the complete pipeline flow: read -> transform -> write."""
        mock_input_df = MagicMock()
        mock_output_df = MagicMock()
        mock_spark.read.table.return_value = mock_input_df
        mock_transform.return_value = mock_output_df

        silver_to_gold.main()

        # Verify the pipeline executed in correct order
        mock_spark.read.table.assert_called_once_with("silver.customers")
        mock_transform.assert_called_once_with(mock_input_df)
        mock_output_df.write.mode.assert_called_once_with("overwrite")
        mock_output_df.write.mode.return_value.saveAsTable.assert_called_once_with(
            "gold.customers_by_country"
        )
