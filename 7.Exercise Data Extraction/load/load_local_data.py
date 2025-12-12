import logging
from fileinput import filename

import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR= PROJECT_ROOT/"output"

def load_to_csv(df: pd.DataFrame,filename:str)-> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_DIR/filename
    logging.info(f"Writing CSV to {file_path}")

    try:
        df.to_csv(file_path,index=False)
    except Exception as e:
        logging.error(f"Failed to write CSV to {file_path}: {e}")
        raise

    logging.info(f"Successfully wrote CSV to {file_path}")


def load_to_json(df: pd.DataFrame,filename:str)-> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_DIR/filename
    logging.info(f"Writing JSON to {file_path}")

    try:
        df.to_json(file_path,orient='records')
    except Exception as e:
        logging.error(f"Failed to write JSON to {file_path}: {e}")
        raise

    logging.info(f"Successfully wrote JSON to {file_path}")