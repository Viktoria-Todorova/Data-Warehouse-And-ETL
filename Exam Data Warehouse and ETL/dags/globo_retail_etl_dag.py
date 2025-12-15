from io import StringIO

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.sdk import dag,task,task_group
import yaml
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.etl.extract_s3 import get_storage_options, extract_csv_data_from_s3, extract_json_from_s3
from include.etl.load_s3 import load_df_to_s3_csv
from include.etl.transform import transform_sales_data, transform_product_data, merge_data

with open("include/config.yaml") as f:
    config = yaml.safe_load(f)

@dag(
    schedule= None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags =["globo_retail_etl_dag"]
)
def globo_retail_etl_dag():
    _, storage_options = get_storage_options(config["aws_conn_id"])

    # @task
    # def debug_s3_access(): #TODO
    #
    #
    #     hook = S3Hook(aws_conn_id=config["aws_conn_id"])
    #
    #     buckets = hook.get_conn().list_buckets()
    #     print([b["Name"] for b in buckets["Buckets"]])

    @task_group(group_id="extract_data")
    def extract_data_group():

        """
            Extract CSV and JSON files from S3 and identify sales and product data paths.
        """

        @task
        def extract_csv_file(bucket:str, folder:str,aws_conn_id:str)-> list:
            return extract_csv_data_from_s3(bucket,folder,aws_conn_id)

        @task
        def extract_json_file(bucket:str, folder:str,aws_conn_id:str)-> list:
            return extract_json_from_s3(bucket,folder,aws_conn_id)

        @task
        def get_sales_path(paths: list) -> str:
            if not paths:
                raise AirflowException("No CSV paths provided")

            for path in paths:
                if "sales_data" in path.lower():
                    return path
            raise AirflowException("sales path not found")

        @task
        def get_product_path(paths: list) -> str:
            for path in paths:
                if "product_data" in path.lower():
                    return path
            raise AirflowException("product path not found")



        csv_path= extract_csv_file(config["s3"]["bucket"],config["s3"]["folder"],config["aws_conn_id"])
        json_path = extract_json_file(config["s3"]["bucket"],config["s3"]["folder"],config["aws_conn_id"])
        sales_path = get_sales_path(csv_path)
        product_path = get_product_path(json_path)

        return {
            "sales_path": sales_path,
            "product_path": product_path
        }


    @task_group(group_id="Transform_group")
    def transform_group(sales_path: str, product_path: str):

        """
            Transform and clean sales and product data, then merge them by product_id.
        """

        @task
        def transform_sales(sales_path: str):
            df = pd.read_csv(sales_path, storage_options=storage_options)
            df = transform_sales_data(df)

            return df.to_json(orient="split")

        @task
        def transform_products(products_path: str):
            df = pd.read_json(products_path, storage_options=storage_options)
            df = transform_product_data(df)
            df['launch_date'] = pd.to_datetime(df['launch_date']).dt.strftime('%Y-%m-%d')
            return df.to_json(orient="split")

        @task
        def merged_data_task(transformed_sales: str, transformed_products: str) -> str:
            sales_df = pd.read_json(StringIO(transformed_sales), orient="split")
            product_df = pd.read_json(StringIO(transformed_products), orient="split")
            merged_df = merge_data(sales_df=sales_df, product_df=product_df)
            return merged_df.to_json(orient="split")

        cleaned_sales = transform_sales(sales_path)
        cleaned_products = transform_products(product_path)
        merged_product_sales = merged_data_task(cleaned_sales, cleaned_products)
        return {
            "cleaned_sales": cleaned_sales,
            "cleaned_products": cleaned_products,
            "merged_product_sales": merged_product_sales
        }

    @task_group(group_id="Load_group")
    def load_group(cleaned_sales:str, cleaned_products:str, merged_product_sales:str):
        """
           Load transformed DataFrames to S3 as CSV files in the output folder.
        """
        @task
        def load_sales_to_s3(cleaned_sales_json: str):
            df = pd.read_json(StringIO(cleaned_sales_json), orient="split")
            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_sales.csv"
            load_df_to_s3_csv(df, output_path, config["aws_conn_id"])
            return output_path

        @task
        def load_products_to_s3(cleaned_products_json: str):
            df = pd.read_json(StringIO(cleaned_products_json), orient="split")
            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_products.csv"
            load_df_to_s3_csv(df, output_path, config["aws_conn_id"])
            return output_path

        @task
        def load_merged_to_s3(merged_json: str):
            df = pd.read_json(StringIO(merged_json), orient="split")
            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/merged_products_sales.csv"
            load_df_to_s3_csv(df, output_path, config["aws_conn_id"])
            return output_path

        sales_output = load_sales_to_s3(cleaned_sales)
        product_output = load_products_to_s3(cleaned_products)
        merged_output = load_merged_to_s3(merged_product_sales)
        return {
            "sales_output": sales_output,
            "product_output": product_output,
            "merged_output": merged_output
        }

    # ETL Pipeline: Extract from S3 → Transform & Clean → Load back to S3
    # Data flows: raw files → validated paths → cleaned DataFrames (JSON) → CSV outputs

    # debug_s3_access()
    extract_output = extract_data_group()
    sales_path =extract_output["sales_path"]
    product_path = extract_output["product_path"]

    transform_output =transform_group(sales_path, product_path)
    cleaned_sales = transform_output["cleaned_sales"]
    cleaned_products = transform_output["cleaned_products"]
    merged_product_sales = transform_output["merged_product_sales"]
    load_output = load_group(cleaned_sales, cleaned_products, merged_product_sales)


globo_retail_etl_dag()
