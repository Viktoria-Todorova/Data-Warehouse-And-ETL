import logging
import pandas as pd
import pyarrow as pa

from config.s3_utils import get_s3_client_and_storage_options


def extract_csv_from_s3(bucket_name:str,file_key:str) -> pd.DataFrame:
    """
    Read CSV from S3 using pandas
    """

    _,storage_options= get_s3_client_and_storage_options()

    s3_path = f's3://{bucket_name}/{file_key}'
    logging.info(f"Extracting CSV from S3{s3_path}")


    try:
        df= pd.read_csv(s3_path,storage_options=storage_options)
    except Exception as e:
        logging.error(f'Failed to extract CSV from S3: {e}')
        raise

    logging.info(f"Successfully extracted CSV from S3: {s3_path}")
    return df

def extract_json_from_s3(bucket_name:str,file_key:str) -> pd.DataFrame:
    """
    Read JSON from S3 using pandas
    """
    _,storage_options= get_s3_client_and_storage_options()
    s3_path = f's3://{bucket_name}/{file_key}'
    logging.info(f"Extracting JSON from S3{s3_path}")

    try:
        df= pd.read_json(s3_path,storage_options=storage_options)
    except Exception as e:
        logging.error(f'Failed to extract JSON from S3: {e}')
        raise

    logging.info(f"Successfully extracted JSON from S3: {s3_path}")
    return df

def extract_parquet_from_s3(bucket_name:str,file_key:str) -> pd.DataFrame:
    """
    Read Parquet from S3 using pandas
    """
    _,storage_options= get_s3_client_and_storage_options()
    s3_path = f's3://{bucket_name}/{file_key}'
    logging.info(f"Extracting Parquet from S3 {s3_path}")

    try:
        df= pd.read_parquet(s3_path,storage_options=storage_options)
    except Exception as e:
        logging.error(f'Failed to extract Parquet from S3: {e}')
        raise
    logging.info(f"Successfully extracted Parquet from S3: {s3_path}")
    return df
