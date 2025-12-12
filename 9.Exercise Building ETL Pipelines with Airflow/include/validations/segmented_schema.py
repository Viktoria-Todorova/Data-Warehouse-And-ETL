import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaError


from ..logger import setup_logger
logging = setup_logger("etl.validations.segment")

segment_customers_schema = pa.DataFrameSchema({
    "customer_id":Column(int, Check.greater_than(0)),
    "total_spent": Column(float, Check.greater_than_or_equal_to(0)),
    "customer_segment" : Column(str, Check.isin(["Low","Medium","High","VIP"])),
    "segmentation_date": Column(pa.DateTime),
})

def validate_post_segmentation_shema(df:pd.DataFrame) -> pd.DataFrame:
    return segment_customers_schema.validate(df)