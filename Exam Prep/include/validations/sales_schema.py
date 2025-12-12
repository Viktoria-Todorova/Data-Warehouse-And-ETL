import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaError

sales_input_schema = pa.DataFrameSchema(
    {
        "sales_id": Column(int),
        "product_id": Column(int),
        "region": Column(str),
        "quantity": Column(int),
        "price": Column(float),
        "timestamp": Column(pa.DateTime),
        "total_sales": Column(float),
    }
)

sales_output_schema = pa.DataFrameSchema(
    {
        "sales_id": Column(int),
        "product_id": Column(int),
        "region": Column(str),
        "quantity": Column(int, Check.greater_than_or_equal_to(0)),
        "price": Column(float,Check.greater_than_or_equal_to(0)),
        "timestamp": Column(pa.DateTime),
        "total_sales": Column(float,Check.greater_than_or_equal_to(0)),
    }
)

def validate_input_sales_schema(sales_df:pd.DataFrame) -> pd.DataFrame:
    try:
        return  sales_input_schema.validate(sales_df)
    except SchemaError as e:
        print(f"Pre-sales schema error: {e}")
        return sales_df

def validate_output_sales_schema(sales_df:pd.DataFrame) -> pd.DataFrame:

    return sales_output_schema.validate(sales_df)
   