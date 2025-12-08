import pandas as pd
from airflow.decorators import dag,task
from airflow.utils import yaml
from pendulum import datetime

from include.etl.extract_data import extract_data_from_s3

with open("include/config.yaml", "r") as f:
    config = yaml.safe_load(f)

@dag(
    start_date=datetime(2025, 1, 1),
    schedule = "@daily",
    catchup = False, # when it's false it skips the skipped days
    tags =["exercise"]
)
def etl_pipeline_dag():
    @task()
    def extract_data(bucket:str,folder:str,aws_conn_id: str)->dict:
        return extract_data_from_s3(bucket=bucket,folder=folder,aws_conn_id=aws_conn_id)

    files = extract_data(
        bucket=config["s3"]["bucket"],
        folder=config["s3"]["folder"],
        aws_conn_id=config["aws_conn_id"]
    )
etl_pipeline_dag()
