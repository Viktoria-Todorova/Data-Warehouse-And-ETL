import pandas as pd
import logging
from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors
import pandera.pandas as pa

sales_scema = DataFrameSchema({
        "order_id": Column(int, checks=Check(lambda s: s>0,error="Order id must be greater than 0")),
        "customer_id" :Column(int, checks=Check(lambda s: s>0,error="Customer id must be greater than 0")),
        "amount" : Column(float,checks=Check(lambda s: s>0,error="Amount must be greater than 0")),
        "quantity" : Column(int,checks=Check(lambda s: s>0,error="Quantity must be greater than 0")),
        "order_date" : Column(pa.DateTime,coerce=True),
    },
    strict=True
    )

def validate_sales(df: pd.DataFrame,lazy:bool=True) -> pd.DataFrame:
    logging.info("Validating sales ...")
    try:
        validate_df = sales_scema.validate(df, lazy=lazy)
    except SchemaErrors as e:
        logging.error("Sales DataFrame Validation Failed")
        logging.error(e.failure_cases)
        raise e

    return validate_df