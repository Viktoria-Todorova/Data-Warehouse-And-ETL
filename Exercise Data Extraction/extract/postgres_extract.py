import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def extract_sales_from_database(sql_query:str,db_params:dict) -> pd.DataFrame:
    connection_string = (f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
                         f"@{db_params['host']}:{db_params['port']}/{db_params['database']}")

    logging.info(f"Connecting to PostgreSQL database {connection_string}")

    try:
        engine = create_engine(connection_string)
    except Exception as e:
        logging.error(f"Unable to connect to PostgreSQL database {connection_string}")
        raise

    logging.info(f"SQL query: {sql_query}")

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(sql_query, con=conn)
    except Exception as e:
        logging.error(f"failed to execute sql query: {sql_query}")
        raise

    logging.info(f"Extracted sales from database")
    return df

