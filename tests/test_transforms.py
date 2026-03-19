import pytest
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from local_pyspark_testing.environment import spark
from local_pyspark_testing.transforms import country_code_to_name, country_name_to_code


@pytest.fixture(scope="module")
def spark_session():
    yield spark


class TestCountryCodeToName:
    def test_converts_valid_codes(self, spark_session):
        df = spark_session.createDataFrame(
            [("GB",), ("DE",), ("US",)],
            ["country_code"],
        )
        expected = spark_session.createDataFrame(
            [("GB", "United Kingdom"), ("DE", "Germany"), ("US", "United States")],
            ["country_code", "country_name"],
        )

        result = df.withColumn("country_name", country_code_to_name("country_code"))

        assertDataFrameEqual(result, expected)

    def test_converts_xs_to_scousia(self, spark_session):
        df = spark_session.createDataFrame([("XS",)], ["country_code"])
        expected = spark_session.createDataFrame(
            [("XS", "Scousia")],
            ["country_code", "country_name"],
        )

        result = df.withColumn("country_name", country_code_to_name("country_code"))

        assertDataFrameEqual(result, expected)

    def test_converts_lowercase_xs_to_scousia(self, spark_session):
        df = spark_session.createDataFrame([("xs",)], ["country_code"])
        expected = spark_session.createDataFrame(
            [("xs", "Scousia")],
            ["country_code", "country_name"],
        )

        result = df.withColumn("country_name", country_code_to_name("country_code"))

        assertDataFrameEqual(result, expected)

    def test_converts_mixed_case_xs_to_scousia(self, spark_session):
        df = spark_session.createDataFrame([("Xs",)], ["country_code"])
        expected = spark_session.createDataFrame(
            [("Xs", "Scousia")],
            ["country_code", "country_name"],
        )

        result = df.withColumn("country_name", country_code_to_name("country_code"))

        assertDataFrameEqual(result, expected)

    def test_handles_lowercase(self, spark_session):
        df = spark_session.createDataFrame([("gb",)], ["country_code"])
        expected = spark_session.createDataFrame(
            [("gb", "United Kingdom")],
            ["country_code", "country_name"],
        )

        result = df.withColumn("country_name", country_code_to_name("country_code"))

        assertDataFrameEqual(result, expected)

    def test_returns_null_for_invalid_code(self, spark_session):
        df = spark_session.createDataFrame([("XX",)], ["country_code"])
        schema = StructType([
            StructField("country_code", StringType()),
            StructField("country_name", StringType()),
        ])
        expected = spark_session.createDataFrame([("XX", None)], schema)

        result = df.withColumn("country_name", country_code_to_name("country_code"))

        assertDataFrameEqual(result, expected)

    def test_returns_null_for_null_input(self, spark_session):
        schema_in = StructType([StructField("country_code", StringType())])
        df = spark_session.createDataFrame([(None,)], schema_in)
        schema_out = StructType([
            StructField("country_code", StringType()),
            StructField("country_name", StringType()),
        ])
        expected = spark_session.createDataFrame([(None, None)], schema_out)

        result = df.withColumn("country_name", country_code_to_name("country_code"))

        assertDataFrameEqual(result, expected)


class TestCountryNameToCode:
    def test_converts_valid_names(self, spark_session):
        df = spark_session.createDataFrame(
            [("United Kingdom",), ("Germany",)],
            ["country_name"],
        )
        expected = spark_session.createDataFrame(
            [("United Kingdom", "GB"), ("Germany", "DE")],
            ["country_name", "country_code"],
        )

        result = df.withColumn("country_code", country_name_to_code("country_name"))

        assertDataFrameEqual(result, expected)

    def test_converts_scousia_to_xs(self, spark_session):
        df = spark_session.createDataFrame([("Scousia",)], ["country_name"])
        expected = spark_session.createDataFrame(
            [("Scousia", "XS")],
            ["country_name", "country_code"],
        )

        result = df.withColumn("country_code", country_name_to_code("country_name"))

        assertDataFrameEqual(result, expected)

    def test_converts_scousia_case_insensitive(self, spark_session):
        df = spark_session.createDataFrame(
            [("scousia",), ("SCOUSIA",), ("ScOuSiA",)],
            ["country_name"],
        )
        expected = spark_session.createDataFrame(
            [("scousia", "XS"), ("SCOUSIA", "XS"), ("ScOuSiA", "XS")],
            ["country_name", "country_code"],
        )

        result = df.withColumn("country_code", country_name_to_code("country_name"))

        assertDataFrameEqual(result, expected)

    def test_returns_null_for_invalid_name(self, spark_session):
        df = spark_session.createDataFrame([("Mancunia",)], ["country_name"])
        schema = StructType([
            StructField("country_name", StringType()),
            StructField("country_code", StringType()),
        ])
        expected = spark_session.createDataFrame([("Mancunia", None)], schema)

        result = df.withColumn("country_code", country_name_to_code("country_name"))

        assertDataFrameEqual(result, expected)

    def test_returns_null_for_null_input(self, spark_session):
        schema_in = StructType([StructField("country_name", StringType())])
        df = spark_session.createDataFrame([(None,)], schema_in)
        schema_out = StructType([
            StructField("country_name", StringType()),
            StructField("country_code", StringType()),
        ])
        expected = spark_session.createDataFrame([(None, None)], schema_out)

        result = df.withColumn("country_code", country_name_to_code("country_name"))

        assertDataFrameEqual(result, expected)
