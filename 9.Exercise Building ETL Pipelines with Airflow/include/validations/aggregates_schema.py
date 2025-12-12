import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaError
from ..logger import setup_logger
logging = setup_logger("etl.validations.aggregates")

pre_aggregates_schema = pa.DataFrameSchema({
    "order_date": Column(pa.DateTime),
    "total_sales":Column(float, Check.greater_than_or_equal_to(0)),
    "unique_customers":Column(int, Check.greater_than(0)),
})

post_aggregates_schema = pa.DataFrameSchema({
    "order_date": Column(pa.DateTime),
    "total_sales": Column(float, Check.greater_than_or_equal_to(0)),
    "unique_customers": Column(int, Check.greater_than(0)),
})

def validate_pre_aggregate_schema(df: pd.DataFrame) -> pd.DataFrame:
    try:
        return pre_aggregates_schema.validate(df)
    except SchemaError as e:
        logging.warning(f"Pre-aggregate schema validation failed: {e}")
        return df

def validate_post_aggregate_schema(df: pd.DataFrame) -> pd.DataFrame:
    return post_aggregates_schema.validate(df)
