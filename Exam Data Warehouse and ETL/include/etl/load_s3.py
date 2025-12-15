import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from include.etl.extract_s3 import get_storage_options
from ..logger import setup_logger
logging= setup_logger("etl.load_data")

def load_df_to_s3_csv(df: pd.DataFrame, s3_path:str,aws_conn_id:str):
    """
        Upload  DataFrame to S3 as a CSV file.
    """
    s3_hook, storage_options = get_storage_options(aws_conn_id)

    try:
        df.to_csv(s3_path, index=False, storage_options=storage_options)
    except Exception as err:
        print(f"Error uploading vsv to S3 {s3_path}: {err}")

    print(f'Finished uploading CSV to S3 {s3_path}')



