import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check


sales_output_schema = pa.DataFrameSchema(
    {
        "sales_id": Column(int),
        "product_id": Column(int),
        "region": Column(str),
        "quantity": Column(int, Check.greater_than_or_equal_to(0)),
        "price": Column(float,Check.greater_than_or_equal_to(0)),
        "timestamp": Column(pa.DateTime),
        "discount": Column(float,Check.greater_than_or_equal_to(0)),
        "order_status": Column(pa.String, Check.isin(["Completed","Pending","Returned","Shipped"]))

    }
)

def validate_output_sales_schema(sales_df:pd.DataFrame) -> pd.DataFrame:
    """
            Validate transformed sales data
    """
    return sales_output_schema.validate(sales_df)