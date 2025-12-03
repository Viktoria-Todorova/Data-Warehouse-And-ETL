#extract all data
import json
import logging
import pandas as pd

def extract_customers_form_json(file_path:str) ->pd.DataFrame:

    """
    Load customers from JSON file into a DataFrame
    """
    logging.info(f'Extracting customers form json file: {file_path}')

    try:
        with open(file_path, "r") as json_file:
            data = json.load(json_file)
    except Exception as e:
        logging.error(f'Error reading json file: {file_path}: {e}')
        raise

    try:
        df = pd.DataFrame(data)
    except Exception as e:
        logging.error(f'Error converting data from JSON file:{file_path}: {e}')
        raise

    logging.info(f"Customers loaded. Shape: {df.shape}")
    return df

def extract_and_flatten_orders(file_path:str) ->pd.DataFrame:
    """
       Load orders from JSON file into DataFrame
   """

    logging.info(f'Extracting orders from json file: {file_path}')
    try:
        with open(file_path, "r") as json_file:
            data = json.load(json_file)
    except Exception as e:
        logging.error(f'Error reading json file: {file_path}: {e}')
        raise

    try:
        df= pd.json_normalize(
            data,
            meta=["order_id","customer_id"],
            record_path=["order_details"],
        )
    except Exception as e:
        logging.error(f'Error flattening orders json file: {file_path}: {e}')
        raise
    logging.info(f"Orders loaded. Shape: {df.shape}")
    return df
