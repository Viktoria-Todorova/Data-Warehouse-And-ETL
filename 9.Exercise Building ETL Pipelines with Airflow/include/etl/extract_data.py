import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from ..logger import setup_logger

logging =setup_logger("etl.extract_data")

def get_storage_options(aws_conn_id:str):

    """
    RETURN AWS creditentials
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    creds = s3_hook.get_credentials()
    storage_options = {
        "key": creds.access_key,
        "secret": creds.secret_key,
    }
    return s3_hook, storage_options

def extract_data_from_s3(bucket:str,folder:str,aws_conn_id: str)->dict:
    """"
    EXTRACT DATA from S3 BUCKET
    """
    s3_hook, storage_options= get_storage_options(aws_conn_id)
    keys= s3_hook.list_keys(bucket_name= bucket,prefix=folder)

    if not keys:
        raise ValueError(f"No fiels found in bucket {bucket} with prefix {folder}")

    dfs = {}

    for key in keys:
        if not key.lower().endswith(".csv"):
            logging.info(f"Skipping non CSV file {key}")
            continue
        s3_path = f"s3://{bucket}/{key}"

        logging.info(f"Reading data from {s3_path}")

        try:
            df= pd.read_csv(s3_path,storage_options=storage_options)
            if df.empty:
                logging.warning(f"File is empty: skipping {s3_path}")
                continue
        except Exception as e:
            logging.error(f"Failed to read CSV file {s3_path}: {e}")


        dfs[key] = df
    return dfs