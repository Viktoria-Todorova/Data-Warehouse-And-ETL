import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check

product_sales_output_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "revenue": Column(float,Check.ge(0)),
    "sales_count": Column(int,Check.ge(0)),
    "value_bucket": Column(str,Check.isin(["Low Performance", "Average","Bestseller"])),
})

def validate_product_sales_rankicng(df:pd.DataFrame)->pd.DataFrame:
    return product_sales_output_schema.validate(df)