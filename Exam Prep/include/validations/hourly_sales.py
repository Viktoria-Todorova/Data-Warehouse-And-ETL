import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check

hourly_sales_trend_schema = pa.DataFrameSchema({
    "region":Column(str,Check(lambda s: s.str.len() >0)),
    "hour": Column(int,Check.in_range(0,23)),
    "hourly_sales_trend" : Column(float,Check.ge(0)),

})

def validate_output_hourly_sales_trend_schema(df: pd.DataFrame) -> pd.DataFrame:
    return hourly_sales_trend_schema.validate(df)