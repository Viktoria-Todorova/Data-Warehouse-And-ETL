import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaError
from ..logger import setup_logger
logging = setup_logger("etl.validations.products")

pre_products_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "product_name": Column(str),
    "category": Column(str),
    "price": Column(float),
})

post_products_schema = pa.DataFrameSchema({
    "product_id": Column(int, Check.greater_than(0)),
    "product_name": Column(str),
    "category": Column(str),
    "price": Column(float, Check.greater_than_or_equal_to(0)),
})
def validate_pre_product_schema(df:pd.DataFrame)->pd.DataFrame:
    try:
        return pre_products_schema.validate(df)
    except SchemaError as e:
        logging.warning(f"Pre-product schema validation failed: {e}")
        return df

def validate_post_product_schema(df:pd.DataFrame)->pd.DataFrame:
    return post_products_schema.validate(df)