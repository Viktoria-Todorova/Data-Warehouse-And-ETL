import pandas as pd
import logging
from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors

order_scema = DataFrameSchema({
        "order_id": Column(int, checks=Check(lambda s: s>0,error="Order id must be greater than 0")),
        "customer_id" :Column(int, checks=Check(lambda s: s>0,error="Customer id must be greater than 0")),
        "product" : Column(str,checks=Check.str_length(1,100,error="Product name must be between 1 and 100 chars")),
        "quantity" : Column(int,checks=Check(lambda s: s>0,error="Quantity must be greater than 0")),
        "price" : Column(float,checks=Check(lambda s: s>0.0,error="Price must be greater than 0")),
    },
    strict=True
    )

def validate_orders(df: pd.DataFrame,lazy:bool=True) -> pd.DataFrame:
    logging.info("Validating orders ...")
    try:
        validate_df = order_scema.validate(df, lazy=lazy)
    except SchemaErrors as e:
        logging.error("Order DataFrame Validation Failed")
        logging.error(e.failure_cases)
        raise e

    return validate_df