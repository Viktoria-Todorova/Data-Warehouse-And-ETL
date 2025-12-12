import logging
import pandas as pd

from config.s3_utils import get_s3_client_and_storage_options


def load_df_to_s3_csv(df: pd.DataFrame,s3_path:str) -> None:
    _,storage_options = get_s3_client_and_storage_options()
    logging.info(f"Writing csv to {s3_path}")

    try:
        df.to_csv(s3_path,index=False, storage_options=storage_options)
    except Exception as e:
        logging.error(f"Failed to write to {s3_path}")
        raise

    logging.info(f"Successfully wrote csv to {s3_path}")

def load_df_to_s3_json(df: pd.DataFrame,s3_path:str) -> None:
    _, storage_options = get_s3_client_and_storage_options()
    logging.info(f"Writing JSON to {s3_path}")

    try:
        df.to_json(s3_path, index=False, storage_options=storage_options)
    except Exception as e:
        logging.error(f"Failed to write to {s3_path}")
        raise

    logging.info(f"Successfully wrote JSON to {s3_path}")