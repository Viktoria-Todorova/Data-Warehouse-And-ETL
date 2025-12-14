from io import StringIO

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.sdk import dag,task,task_group
import yaml
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.etl.extract_s3 import get_storage_options, extract_csv_data_from_s3, extract_json_from_s3
from include.etl.load_s3 import load_df_to_s3_csv, load_data_to_snowflake
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
    s3_hook, storage_options = get_storage_options(config["aws_conn_id"])

    @task
    def debug_s3_access():


        hook = S3Hook(aws_conn_id=config["aws_conn_id"])

        buckets = hook.get_conn().list_buckets()
        print([b["Name"] for b in buckets["Buckets"]])

    @task_group(group_id="extract_data")
    def extract_data_group():
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
        @task
        def transform_sales(sales_path: str):
            df = pd.read_csv(sales_path, storage_options=storage_options)
            df = transform_sales_data(df)
            output_path= f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_sales.csv"
            load_df_to_s3_csv(df,output_path,config["aws_conn_id"])
            return df.to_json(orient="split")

        @task
        def transform_products(products_path: str):
            df = pd.read_json(products_path, storage_options=storage_options)
            df = transform_product_data(df)
            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_products.csv"
            load_df_to_s3_csv(df, output_path, config["aws_conn_id"])
            return df.to_json(orient="split")

        cleaned_sales = transform_sales(sales_path)
        cleaned_products = transform_products(product_path)

        return {
            "cleaned_sales": cleaned_sales,
            "cleaned_products": cleaned_products
        }
    @task
    def merged_data_task(transformed_sales:str, transformed_products:str) ->str:
        sales_df = pd.read_json(StringIO(transformed_sales),orient="split")
        product_df = pd.read_json(StringIO(transformed_products),orient="split")
        merged_df =merge_data(sales_df = sales_df, product_df = product_df)
        output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/merged_products_sales.csv"
        load_df_to_s3_csv(merged_df, output_path, config["aws_conn_id"])
        return merged_df.to_json(orient="split")

    @task
    def load_to_snowflake(final_json:str,database:str,schema_name:str,table_name:str):
        final_df = pd.read_json(StringIO(final_json), orient="split")
        load_data_to_snowflake(df=final_df,database=database,schema= schema_name,table=table_name)

    @task_group
    def load_group(transform_output: dict,merge_output:str):
        """Load all cleaned and merged data to Snowflake"""

        # Load sales data
        load_sales = load_to_snowflake.override(task_id="load_sales")(
            final_json=transform_output["cleaned_sales"],
            database=config["snowflake"]["database"],
            schema_name=config["snowflake"]["targets"]["sales"]["schema"],
            table_name=config["snowflake"]["targets"]["sales"]["table"]
        )

        # Load products data
        load_products = load_to_snowflake.override(task_id="load_products")(
            final_json=transform_output["cleaned_products"],
            database=config["snowflake"]["database"],
            schema_name=config["snowflake"]["targets"]["products"]["schema"],
            table_name=config["snowflake"]["targets"]["products"]["table"]
        )

        # Load merged data
        load_merged = load_to_snowflake.override(task_id="load_merged")(
            final_json=merge_output,
            database=config["snowflake"]["database"],
            schema_name=config["snowflake"]["targets"]["merged"]["schema"],
            table_name=config["snowflake"]["targets"]["merged"]["table"]
        )

        return {
            "sales_loaded": load_sales,
            "products_loaded": load_products,
            "merged_loaded": load_merged
        }

    debug_s3_access()
    extract_output = extract_data_group()
    sales_path =extract_output["sales_path"]
    product_path = extract_output["product_path"]

    transform_output =transform_group(sales_path, product_path)

    merge_output = merged_data_task(transform_output["cleaned_sales"],
                                    transform_output["cleaned_products"])

    load_group(transform_output,merge_output)

globo_retail_etl_dag()
