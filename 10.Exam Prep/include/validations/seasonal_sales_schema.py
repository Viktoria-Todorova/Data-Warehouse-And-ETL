import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check

seasonal_sales_pattern_schema = pa.DataFrameSchema({
    "quarter": Column(str),
    "category": Column(str),
    "total_sales": Column(float,Check.ge(0)),

})

def validate_output_seasonal_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    return seasonal_sales_pattern_schema.validate(df)