import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaError

product_input_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "category": Column(str),
    "brand": Column(str),
    "rating": Column(float),
    "in_stock":Column(bool),
    "launch_date":Column(pa.DateTime)
})


product_output_schema =pa.DataFrameSchema({
    "product_id": Column(int),
    "category": Column(str),
    "brand": Column(str),
    "rating": Column(float,Check(lambda s: s.between(0,5))),
    "in_stock":Column(bool),
    "launch_date":Column(pa.DateTime)
})

def validate_input_product_schema(product_df:pd.DataFrame) -> pd.DataFrame:
    try:
        product_input_schema.validate(product_df)
    except SchemaError as e:
        print(f"Pre-product shema validation failed: {e}")
    return product_df

def validate_output_product_schema(product_df:pd.DataFrame) -> pd.DataFrame:
    return product_output_schema.validate(product_df)