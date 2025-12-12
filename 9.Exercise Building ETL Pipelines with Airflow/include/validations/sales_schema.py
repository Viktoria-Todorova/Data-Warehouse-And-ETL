import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaError
from ..logger import setup_logger
logging = setup_logger("etl.validations.sales")

pre_sales_schema = pa.DataFrameSchema({
    "order_id": Column(str),
    "customer_id": Column(int),
    "product_id": Column(int),
    "order_date": Column(pa.DateTime),
    "amount": Column(float),
    "quantity": Column(int),
    "discount": Column(float),
    "profit": Column(float),
    "total_revenue": Column(float),
})

post_sales_schema = pa.DataFrameSchema({
    "order_id": Column(str),
    "customer_id": Column(int,Check.greater_than(0)),
    "product_id": Column(int,Check.greater_than(0)),
    "order_date": Column(pa.DateTime),
    "amount": Column(float,Check.greater_than_or_equal_to(0)),
    "quantity": Column(int,Check.greater_than_or_equal_to(0)),
    "discount": Column(float,Check.between(0,100)),
    "profit": Column(float),
    "total_revenue": Column(float, Check.greater_than_or_equal_to(0)),
})

def validate_pre_sales_schema(df: pd.DataFrame)->pd.DataFrame:
    try:
        return pre_sales_schema.validate(df)
    except SchemaError as e:
        logging.warning(f"Pre-sales schema validation failed: {e.failure_cases}")
        return df

def validate_post_sales_schema(df: pd.DataFrame)->pd.DataFrame:
    return post_sales_schema.validate(df)
