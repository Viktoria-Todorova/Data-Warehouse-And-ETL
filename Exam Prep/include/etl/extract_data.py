
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def get_storage_option(aws_conn_id:str):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    creds = s3_hook.get_credentials()

    storage_options = {
        "key" : creds.access_key,
        "secret" : creds.secret_key
    }

    return s3_hook,storage_options

def extract_data_from_s3(bucket:str, folder:str,aws_conn_id:str,file_type:str = "csv") -> list:
    print(f"Listing {file_type} files in s3://{bucket}/{folder}")
    s3_hook, storage_options = get_storage_option(aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=bucket,prefix=folder)

    if not keys:
        raise AirflowException(f"No files found in s3://{bucket}/{folder}")

    matched_paths = [f"s3://{bucket}/{key}"for key in keys if key.lower().endswith(f".{file_type}")]

    if not matched_paths:
        raise AirflowException(f"No {file_type} found in s3://{bucket}/{folder}")

    return matched_paths
