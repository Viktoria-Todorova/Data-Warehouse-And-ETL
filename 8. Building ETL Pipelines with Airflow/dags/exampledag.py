import io


# from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
from airflow.sdk import dag, task
from airflow.utils import yaml


# Define the basic parameters of the DAG, like schedule and start_date
@dag(schedule =None, start_date=datetime(2023, 1, 1), catchup=False, tags=["astro", "s3", "db"])

def example_astronauts():
    @task
    def extract_from_s3() -> pd.DataFrame:
        """
          ETL pipeline that:
          1. Extracts sales data from S3
          2. Transforms the data (lowercase columns, aggregate by region)
          3. Loads the transformed data into Snowflake
          """

        with open("include/config.yaml", "r") as file:
            config = yaml.safe_load(file)

        s3_hook = S3Hook(aws_conn_id=config['aws_conn_id'])
        bucket = config["s3"]['bucket']
        file = config["s3"]['folder']

        key = s3_hook.get_key(file, bucket)

        if not key:
            raise ValueError(f"No files found in bucket {bucket} with prefix {file}")

        content = key.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(content))

        return df
    @task
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        df["sales"] = df["sales"].astype(float)
        aggregated_df = df.groupby("region").agg({"sales": "sum"}).reset_index()
        return aggregated_df

    @task
    def load_to_snowflake(transformed_df: pd.DataFrame):
        hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
        with open("include/config.yaml", "r") as file:
            config = yaml.safe_load(file)

        for _, row in transformed_df.iterrows():
            insert_sql = f"""
                INSERT INTO {config['snowflake']['table']} (region, sales)
                VALUES (%(region)s, %(sales)s)
            """
            hook.run(insert_sql, parameters={"region": row["region"], "sales": row["sales"]})

    df = extract_from_s3()
    transformed_df = transform(df)
    load_to_snowflake(transformed_df)
# Instantiate the DAG
example_astronauts()
