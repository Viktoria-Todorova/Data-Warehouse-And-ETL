import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from ..logger import setup_logger
logging= setup_logger("etl.load_data")

def load_data_to_snowflake(df: pd.DataFrame, database: str, schema: str, table:str ):
    """ load data to Snowflake tables"""
    if df.empty:
        raise ValueError("empty dataframe")

    try:
        snowflake_hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
        engine = snowflake_hook.get_sqlalchemy_engine()

        df.to_sql(name=table,
                  con=engine,
                  schema=schema,
                  index= False,
                  if_exists='append',
                  method="multi")
                  # chunksize=10000)

    except Exception as e:
        logging.error(f"unable to connect to Snowflake: {e}")
        raise