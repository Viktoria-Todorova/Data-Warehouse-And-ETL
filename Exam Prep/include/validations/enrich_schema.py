import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaError

enriched_output_shema = pa.DataFrameSchema({
        "sales_id": Column(int),
        "product_id": Column(int),
        "region": Column(str),
        "quantity": Column(int),
        "price": Column(float),
        "timestamp": Column(pa.DateTime),
        "total_sales": Column(float),
        "category": Column(str),
        "brand": Column(str),
        "rating": Column(float),
        "month": Column(str),
        "hour": Column(int),
        "sales_bucket": Column(str),
})


def validate_output_enriched_schema(merged_df:pd.DataFrame) -> pd.DataFrame:
    return enriched_output_shema.validate(merged_df)