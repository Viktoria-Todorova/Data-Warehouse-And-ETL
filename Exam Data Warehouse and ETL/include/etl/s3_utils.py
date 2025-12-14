import pandas as pd
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def read_s3_csv(s3_path: str, aws_conn_id: str) -> pd.DataFrame:
    """Read CSV from S3 using S3Hook"""
    path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = path_parts[0]
    key = path_parts[1]

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    file_content = s3_hook.read_key(key=key, bucket_name=bucket)
    return pd.read_csv(StringIO(file_content))


def read_s3_json(s3_path: str, aws_conn_id: str) -> pd.DataFrame:
    """Read JSON from S3 using S3Hook"""
    path_parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = path_parts[0]
    key = path_parts[1]

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    file_content = s3_hook.read_key(key=key, bucket_name=bucket)
    return pd.read_json(StringIO(file_content))