import pandas as pd
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from ..logger import setup_logger

logging = setup_logger("etl.extract_data")


def get_storage_options(aws_conn_id: str):
    """
    RETURN AWS credentials
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    creds = s3_hook.get_credentials()
    storage_options = {
        "key": creds.access_key,
        "secret": creds.secret_key,
    }
    if creds.token:
        storage_options["token"] = creds.token
    return s3_hook, storage_options


def extract_file_from_s3(bucket: str, folder: str, aws_conn_id: str, file_type: str) -> list:
    """
        Extract and validate files from S3 bucket by file type (csv/json).
        Returns list of S3 paths for valid, non-empty files that can be read as DataFrames.
    """

    s3_hook, _ = get_storage_options(aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=folder)

    if not keys:
        raise ValueError(f"No files found in bucket {bucket} with prefix {folder}")

    valid_paths = []
    for key in keys:
        if not key.lower().endswith(f".{file_type}"):
            logging.info(f"Skipping non-{file_type} file {key}")
            continue

        s3_path = f"s3://{bucket}/{key}"
        logging.info(f"Reading data from {s3_path}")

        try:

            file_content = s3_hook.read_key(key=key, bucket_name=bucket)

            if file_type == "csv":
                df = pd.read_csv(StringIO(file_content))
            elif file_type == "json":
                df = pd.read_json(StringIO(file_content))
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            if df.empty:
                logging.warning(f"File is empty: skipping {s3_path}")
                continue

            valid_paths.append(s3_path)

        except Exception as e:
            logging.error(f"Failed to read {file_type} file {s3_path}: {e}")
            continue

    return valid_paths


def extract_csv_data_from_s3(bucket: str, folder: str, aws_conn_id: str) -> list:
    """
    extract csv data from S3 bucket
    """
    return extract_file_from_s3(bucket, folder, aws_conn_id, file_type="csv")


def extract_json_from_s3(bucket: str, folder: str, aws_conn_id: str) -> list:
    """
    Read JSON from S3
    """
    return extract_file_from_s3(bucket, folder, aws_conn_id, file_type="json")