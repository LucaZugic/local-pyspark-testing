import pycountry
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

CUSTOM_CODES = {"XS": "Scousia"}
CUSTOM_NAMES = {"scousia": "XS"}


@udf(returnType=StringType())
def country_code_to_name(alpha_2: str) -> str | None:
    """Convert ISO alpha-2 country code to full country name."""
    if not alpha_2:
        return None
    upper_code = alpha_2.upper()
    if upper_code in CUSTOM_CODES:
        return CUSTOM_CODES[upper_code]
    country = pycountry.countries.get(alpha_2=upper_code)
    return country.name if country else None


@udf(returnType=StringType())
def country_name_to_code(name: str) -> str | None:
    """Convert country name to ISO alpha-2 code."""
    if not name:
        return None
    lower_name = name.lower()
    if lower_name in CUSTOM_NAMES:
        return CUSTOM_NAMES[lower_name]
    try:
        country = pycountry.countries.lookup(name)
        return country.alpha_2
    except LookupError:
        return None
