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

def validate_input_sales_schema(sales_df:pd.DataFrame) -> pd.DataFrame:
    try:
    except SchemaError as e:
        print(f"Pre-sales schema error: {e}")