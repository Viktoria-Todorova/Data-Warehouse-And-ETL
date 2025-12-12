import pandera as pa
import pandas as pd
from pandera import Column,DataFrameSchema, Check

sales_schema = DataFrameSchema(
    {
        "order_id": Column(
            int,
            checks=Check.ge(-1),
            nullable=False,
        ),
        "customer_id": Column(
            int,
            checks=Check.ge(-1),
            nullable=False,
        ),
        "amount": Column(
            float,
            checks=Check.ge(0),
            nullable=False,
        ),
        "quantity": Column(
            int,
            checks=Check.ge(0),
            nullable=False,
        ),
        "order_date": Column(
            pa.DateTime,
            nullable=True,
        ),
        "total_revenue": Column(
            float,
            checks=Check.ge(0),
            nullable=False,
        ),
    },
    strict=True,  # reject extra columns not defined here
)

def validate_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    validated_df = sales_schema.validate(df)
    return validated_df
