import logging
import pandas as pd
from pandera.errors import SchemaErrors

from pandera.pandas import DataFrameSchema, Column, Check

weather_schema = DataFrameSchema(
    {
        "city":Column(str,checks=Check.str_length(1,100)),
        "temperature":Column(str,checks=Check.str_length(1,100,error ="Invalid temperature")),
        "feel": Column(str)
    },

    strict=True,
)

def validate_weather(df: pd.DataFrame,lazy: bool = True) -> pd.DataFrame:
    logging.info("Weather DataFrame Validation")

    try:
        validate_df = weather_schema.validate(df,lazy=lazy)
    except SchemaErrors as e:
        logging.error("Weather DataFrame Validation Failed")
        logging.error(e.failure_cases)
        raise e

    return validate_df

