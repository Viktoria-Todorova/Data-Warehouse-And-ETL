import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check

revenue_concentation_schema = pa.DataFrameSchema({
    "region": Column(str, Check(lambda s: s.str.len() > 0)),
    "region_revenue": Column(float, Check.ge(0)),
    "revenue_share": Column(float, Check.in_range(0,1)),
    "cumulative_share": Column(float),
})

def validate_revenue_concentration_schema(df: pd.DataFrame) -> pd.DataFrame:
    return revenue_concentation_schema.validate(df)
