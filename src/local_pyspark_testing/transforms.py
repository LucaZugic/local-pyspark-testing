import pycountry
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def country_code_to_name(alpha_2: str) -> str | None:
    """Convert ISO alpha-2 country code to full country name."""
    if not alpha_2:
        return None
    country = pycountry.countries.get(alpha_2=alpha_2.upper())
    return country.name if country else None


@udf(returnType=StringType())
def country_name_to_code(name: str) -> str | None:
    """Convert country name to ISO alpha-2 code."""
    if not name:
        return None
    try:
        country = pycountry.countries.lookup(name)
        return country.alpha_2
    except LookupError:
        return None
