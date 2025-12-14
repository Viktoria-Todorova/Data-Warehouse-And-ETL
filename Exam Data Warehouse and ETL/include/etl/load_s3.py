import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from include.etl.extract_s3 import get_storage_options
from ..logger import setup_logger
logging= setup_logger("etl.load_data")

def load_df_to_s3_csv(df: pd.DataFrame, s3_path:str,aws_conn_id:str):
    s3_hook, storage_options = get_storage_options(aws_conn_id)

    try:
        df.to_csv(s3_path, index=False, storage_options=storage_options)
    except Exception as err:
        print(f"Error uploading vsv to S3 {s3_path}: {err}")

    print(f'Finished uploading CSV to S3 {s3_path}')


def load_data_to_snowflake(df: pd.DataFrame, database:str,schema:str,table:str):
    """ load data to Snowflake tables"""
    if df.empty:
        raise ValueError("empty dataframe")

    try:
        logging.info(f"Loading {len(df)} rows to {database}.{schema}.{table}")
        df_copy = df.copy()
        if "launch_date" in df_copy.columns:
            df_copy["launch_date"] = pd.to_datetime(
                df_copy["launch_date"], format="%Y%m%d", errors='coerce'
            )

        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')

        snowflake_hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
        engine = snowflake_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            conn.execute(f"USE DATABASE {database}")
            conn.execute(f"USE SCHEMA {schema}")
            conn.execute(f"TRUNCATE TABLE IF EXISTS {table}")
            df_copy.to_sql(name=table,
                      con=conn,
                      schema=schema,
                      index=False,
                      if_exists='append',
                      method="multi")

        logging.info(f"Successfully loaded {len(df)} rows to {database}.{schema}.{table}")

    except Exception as e:
        logging.error(f"unable to connect to Snowflake: {e}")
        raise

