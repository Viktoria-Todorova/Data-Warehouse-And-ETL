import pandas as pd
from airflow.exceptions import AirflowException
from airflow.sdk import dag,task,task_group
from airflow.utils import yaml
from pendulum import datetime

from include.etl.extract_data import get_storage_option, extract_data_from_s3
from include.etl.load_data import load_df_to_s3_csv
from include.etl.transform import transform_sales_data, transform_product_data, merge_sales_and_product_data, \
    enrich_merged_data, hourly_sales_trend, product_sales_ranking_with_brand, seasonal_sales_pattern, \
    revenue_concentration_pattern

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

    @task_group(group_id="Transform_group")
    def transform_group(sales_path:str, product_path:str):
        @task
        def transform_sales(sales_path:str):
            df= pd.read_csv(sales_path,storage_options=storage_options)
            df= transform_sales_data(df)
            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_sales.csv"
            load_df_to_s3_csv(df,output_path,config["aws_conn_id"])
            return output_path

        @task
        def transform_products(products_path: str):
            df = pd.read_json(products_path, storage_options=storage_options)
            df = transform_product_data(df)
            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_products.csv"
            load_df_to_s3_csv(df, output_path, config["aws_conn_id"])
            return output_path

        @task
        def merge_data(clean_sales_path:str, clean_product_path:str):
            sales_df = pd.read_csv(clean_sales_path, storage_options=storage_options)
            product_df = pd.read_csv(clean_product_path, storage_options=storage_options)
            merge_df = merge_sales_and_product_data(sales_df, product_df)
            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/merge.csv"
            load_df_to_s3_csv(merge_df, output_path, config["aws_conn_id"])
            return output_path

        @task
        def enrich_data(merged_path:str):
            df = pd.read_csv(merged_path, storage_options=storage_options)
            enriched_df = enrich_merged_data(df)
            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/enrich.csv"
            load_df_to_s3_csv(enriched_df, output_path, config["aws_conn_id"])

            return output_path

        clean_sales = transform_sales(sales_path)
        clean_product = transform_products(product_path)
        merge_data_result = merge_data(clean_sales, clean_product)
        enriched_result = enrich_data(merge_data_result)

        return {
            "clean_sales": clean_sales,
            "clean_product": clean_product,
            "merge_data": merge_data_result,
            "enrich_data": enriched_result,
        }

    @task_group(group_id="analytics")
    def analytics_group(enriched_path:str):

        @task
        def run_hourly_sales_trend(enriched_path:str):
            df = pd.read_csv(enriched_path, storage_options=storage_options)
            result= hourly_sales_trend(df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['analytics']}/hourly_sales.csv"
            load_df_to_s3_csv(result, output_path, config["aws_conn_id"])

            return output_path

        @task
        def run_product_sales_trend(enriched_path: str):
           df = pd.read_csv(enriched_path, storage_options=storage_options)
           result = product_sales_ranking_with_brand(df)

           output_path = f"s3://{config['s3']['bucket']}/{config['s3']['analytics']}/product_Sales_ranking.csv"
           load_df_to_s3_csv(result, output_path, config["aws_conn_id"])

           return output_path

        @task
        def run_seasonal_sales_trend(enriched_path: str):
           df = pd.read_csv(enriched_path, storage_options=storage_options)
           result = seasonal_sales_pattern(df)

           output_path = f"s3://{config['s3']['bucket']}/{config['s3']['analytics']}/seasonal_Sales_pattern.csv"
           load_df_to_s3_csv(result, output_path, config["aws_conn_id"])

           return output_path

        @task
        def run_revenue_concentration(enriched_path: str):
            df = pd.read_csv(enriched_path, storage_options=storage_options)
            result = revenue_concentration_pattern(df)

            output_path= f"s3://{config['s3']['bucket']}/{config['s3']['analytics']}/revenue_concentration.csv"
            load_df_to_s3_csv(result, output_path, config["aws_conn_id"])
            return output_path

        hourly_trend =run_hourly_sales_trend(enriched_path)
        product_trend = run_product_sales_trend(enriched_path)
        seasonal_trend = run_seasonal_sales_trend(enriched_path)
        revenue_concentration = run_revenue_concentration(enriched_path)

        return {
            "hourly_trend": hourly_trend,
            "product_trend": product_trend,
            "seasonal_trend": seasonal_trend,
            "revenue_concentration": revenue_concentration,
        }

    @task_group(group_id="load_group")
    def load_group(hourly_trend, product_trend, seasonal_trend, revenue_concentration):

        @task()
        def copy_csv(input_path:str, output_file:str):
            df = pd.read_csv(input_path, storage_options=storage_options)
            bucket = config["s3"]["bucket"]
            folder = config["s3"]["analytics"]
            output_path = f"s3://{bucket}/{folder}/{output_file}"
            load_df_to_s3_csv(df,output_path, config["aws_conn_id"])
            return output_path

        copy_csv.override(task_id= "load_hourly_trend")(hourly_trend,"hourly_sales_trend.csv")
        copy_csv.override(task_id = "load_product_sales_ranking")(product_trend,"product_sales_ranking.csv")
        copy_csv.override(task_id="load_seasonal_patterns")(product_trend, "seasonal_patterns.csv")
        copy_csv.override(task_id="load_revenue_conc")(product_trend, "revenue_conc.csv")



    extract_output = extract_group()
    sales_path = extract_output["sales_path"]
    product_path = extract_output["product_path"]

    transform_output =transform_group(sales_path,product_path)
    enriched_output = transform_output["enrich_data"]
    analytics_output = analytics_group(enriched_output)
    load_group(
        hourly_trend=analytics_output["hourly_trend"],
        product_trend=analytics_output["product_trend"],
        seasonal_trend=analytics_output["seasonal_trend"],
        revenue_concentration=analytics_output["revenue_concentration"],
    )


retail_etl_dag()

