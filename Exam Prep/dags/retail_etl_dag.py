import pandas as pd
from airflow.exceptions import AirflowException
from airflow.sdk import dag,task,task_group
from airflow.utils import yaml
from pendulum import datetime

from include.etl.extract_data import get_storage_option, extract_data_from_s3

with open("include/config.yaml") as f:
    config = yaml.safe_load(f)

@dag(
    schedule= None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags =["retail_etl_dag"]
)
def retail_etl_dag():
    s3_hook, storage_options = get_storage_option(config["aws_conn_id"])

    @task_group(group_id="extract_group")
    def extract_group():
        @task
        def extract_csv_file(bucket:str, folder:str,aws_conn_id:str)->list:
            return extract_data_from_s3(bucket, folder, aws_conn_id,"csv")

        @task
        def extract_json_file(bucket: str, folder: str, aws_conn_id: str) -> list:
            return extract_data_from_s3(bucket, folder, aws_conn_id, "json")

        @task
        def get_sales_path(paths: list)->str:
            for path in paths:
                if "sales" in path.lower():
                    return path
            raise AirflowException("sales path not found")

        @task
        def get_product_path(paths: list)->str:
            for path in paths:
                if "product" in path.lower():
                    return path
                raise AirflowException("product path not found")

        csv_path = extract_csv_file(config["s3"]["bucket"], config["s3"]["folder"], config["aws_conn_id"])
        json_path = extract_json_file(config["s3"]["bucket"], config["s3"]["folder"], config["aws_conn_id"])

        sales_path = get_sales_path(csv_path)
        product_path = get_product_path(json_path)

        return {
            "sales_path": sales_path,
            "product_path": product_path,
        }

    extract_output = extract_group()
    sales_path = extract_output["sales_path"]
    product_path = extract_output["product_path"]

retail_etl_dag()

