import pandas as pd
import logging
from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors

customer_scema = DataFrameSchema({
    "name": Column(str),
    "email": Column(str),
    "customer_id": Column(int, checks=[
        Check(lambda s: s >0, error = "Customer ID must be greater than 0"),
        Check(lambda s: s.is_unique, error = "Customer ID must be unique")]),
    },
    strict=True
    )

def validate_customers(df: pd.DataFrame,lazy:bool=True) -> pd.DataFrame:
    logging.info("Validating customer ...")
    try:
        validate_df = customer_scema.validate(df, lazy=lazy)
    except SchemaErrors as e:
        logging.error("Customer DataFrame Validation Failed")
        logging.error(e.failure_cases)
        raise e

    return validate_df