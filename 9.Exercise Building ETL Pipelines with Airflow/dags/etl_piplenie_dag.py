import io
import json
import pandas as pd
from airflow.sdk import dag, task, TaskGroup
from airflow.utils import yaml
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.etl.extract_data import extract_data_from_s3
from include.etl.load_data import load_data_to_snowflake
from include.etl.transform import clean_sales_data, clean_customers_data, clean_products_data, merge_data, \
    compute_monthly_aggregates, segment_customers, detect_sales_anomalies, forecast_sales

with open("include/config.yaml", "r") as f:
    config = yaml.safe_load(f)
STAGING_FOLDER = "staging/extracted_data"
@dag(
    start_date=datetime(2025, 1, 1),
    schedule = "@daily",
    catchup = False, # when it's false it skips the skipped days
    tags =["exercise"]
)
def etl_pipeline_dag():
    # @task()
    # def extract_data(bucket:str,folder:str,aws_conn_id: str)->dict:
    #     return extract_data_from_s3(bucket=bucket,folder=folder,aws_conn_id=aws_conn_id)

    @task(do_xcom_push=False)
    def extract_data(bucket: str, folder: str, aws_conn_id: str) -> dict:
        """Extract data and save to S3 staging area"""
        # Extract raw data
        raw_data = extract_data_from_s3(bucket=bucket, folder=folder, aws_conn_id=aws_conn_id)

        # Initialize S3 Hook
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)



        # Save each DataFrame to S3 and collect paths
        file_paths = {}
        for key, df in raw_data.items():
            # Create a clean filename from the key
            filename = key.split('/')[-1].replace('.csv', '.parquet')
            s3_key = f"{STAGING_FOLDER}/{filename}"

            # Convert DataFrame to parquet bytes
            parquet_buffer = df.to_parquet(index=False)

            # Upload to S3
            s3_hook.load_bytes(
                bytes_data=parquet_buffer,
                key=s3_key,
                bucket_name=bucket,
                replace=True
            )

            file_paths[key] = f"s3://{bucket}/{s3_key}"

        # Return only the file paths (small data for XCom)
        return file_paths

    # @task()
    # def get_sales_file(files:dict)->str:
    #     for key,df in files.items():
    #         if "sales" in key:
    #             return df.to_json(orient="split")
    #     raise ValueError("Sales file not found")

    @task()
    def get_sales_file(bucket: str, aws_conn_id: str) -> str:
        """Extract sales file from S3 staging (parquet format)"""

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        # List files in staging
        keys = s3_hook.list_keys(bucket_name=bucket, prefix=STAGING_FOLDER)

        # Find the sales parquet file
        for key in keys:
            if "sales" in key.lower() and key.endswith('.parquet'):
                # Get file content from S3
                file_obj = s3_hook.get_key(key, bucket)
                content = file_obj.get()['Body'].read()

                # Read parquet into DataFrame
                df = pd.read_parquet(io.BytesIO(content))

                print(f"Loaded sales data: {len(df)} rows from {key}")
                return df.to_json(orient="split")

        raise ValueError("Sales file not found in S3 staging")


    # @task()
    # def get_customers_file(files: dict) -> str:
    #     for key, df in files.items():
    #         if "customer" in key:
    #             return df.to_json(orient="split")
    #     raise ValueError("Customer file not found")
    #
    # @task()
    # def get_product_file(files: dict) -> str:
    #     for key, df in files.items():
    #         if "product" in key:
    #             return df.to_json(orient="split")
    #     raise ValueError("Product file not found")

    @task()
    def get_customers_file(bucket: str, aws_conn_id: str) -> str:
        """Read customers file from S3 staging"""
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        keys = s3_hook.list_keys(bucket_name=bucket, prefix=STAGING_FOLDER)

        # Find and read customer file
        for key in keys:
            if "customer" in key.lower() and key.endswith('.parquet'):
                # Get file content from S3
                file_obj = s3_hook.get_key(key, bucket)
                content = file_obj.get()['Body'].read()

                # ✅ Wrap bytes in BytesIO before reading parquet
                df = pd.read_parquet(io.BytesIO(content))

                print(f"Loaded product data: {len(df)} rows from {key}")
                return df.to_json(orient="split")

        raise ValueError("Customer file not found")

    @task()
    def get_product_file(bucket: str, aws_conn_id: str) -> str:
        """Read customers file from S3 staging"""
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        keys = s3_hook.list_keys(bucket_name=bucket, prefix=STAGING_FOLDER)

        # Find and read customer file
        for key in keys:
            if "product" in key.lower() and key.endswith('.parquet'):
                # Get file content from S3
                file_obj = s3_hook.get_key(key, bucket)
                content = file_obj.get()['Body'].read()

                # ✅ Wrap bytes in BytesIO before reading parquet
                df = pd.read_parquet(io.BytesIO(content))

                print(f"Loaded product data: {len(df)} rows from {key}")
                return df.to_json(orient="split")

        raise ValueError("Product file not found")

    @task
    def transfrom_sales_data(sales_file: str)->str:
        sales_df = pd.read_json(sales_file, orient="split")
        sales_df= clean_sales_data(sales_df = sales_df)
        return sales_df.to_json(orient="split", date_format="iso")

    @task
    def transfrom_customers_data(customers_file: str) -> str:
        customers_df = pd.read_json(customers_file, orient="split")
        customers_df = clean_customers_data(customers_df = customers_df)
        return customers_df.to_json(orient="split", date_format="iso")

    @task
    def transfrom_product_data(products_file: str) -> str:
        products_df = pd.read_json(products_file, orient="split")
        products_df = clean_products_data(products_df = products_df)
        return products_df.to_json(orient="split", date_format="iso")

    @task
    def merget_data_task(transformed_sales:str, transformed_customers:str, transformed_products:str)->str:
        sales_df = pd.read_json(transformed_sales, orient="split")
        customers_df = pd.read_json(transformed_customers, orient="split")
        products_df = pd.read_json(transformed_products, orient="split")
        merge_df= merge_data(sales_df = sales_df, customers_df = customers_df, products_df = products_df)
        return merge_df.to_json(orient="split")

    @task
    def aggregated_data_task(merged_data:str)->str:
        merged_df = pd.read_json(merged_data  , orient="split")
        aggregated_df = compute_monthly_aggregates(merged_df = merged_df)
        return aggregated_df.to_json(orient="split",date_format="iso")

    @task()
    def segment_customers_task(sales:str,customers:str)->str:
        sales_df = pd.read_json(sales, orient="split")
        customers_df = pd.read_json(customers, orient="split")
        segment_df = segment_customers(sales_df = sales_df, customers_df = customers_df)
        return segment_df.to_json(orient="split", date_format="iso")

    @task
    def anomalies_sales_task(sales:str) -> str:
        sales_df = pd.read_json(sales, orient="split")
        sales_df = detect_sales_anomalies(sales_df )
        return sales_df.to_json(orient="split", date_format="iso")
    @task
    def forecasted_sales_task(sales:str)->str:
        sales_df = pd.read_json(sales, orient="split")
        sales_df = forecast_sales(sales_df)
        return sales_df.to_json(orient="split", date_format="iso")
    @task
    def load_to_snowflake_task(final_json:str,database:str,shema_name = str, table_name =str):
        final_df = pd.read_json(final_json, orient="split")
        load_data_to_snowflake(df=final_df, database=database,schema=shema_name, table=table_name)

    with TaskGroup("extraction") as extraction:
        files = extract_data(
            bucket=config["s3"]["bucket"],
            folder=config["s3"]["folder"],
            aws_conn_id=config["aws_conn_id"]
        )
        sales_file = get_sales_file(bucket=config["s3"]["bucket"],aws_conn_id= config["aws_conn_id"])
        customers_file = get_customers_file(config["s3"]["bucket"], config["aws_conn_id"])
        product_file = get_product_file(config["s3"]["bucket"], config["aws_conn_id"])

    with TaskGroup("transform") as transform:
        transform_sales = transfrom_sales_data(sales_file = sales_file)
        transform_customers = transfrom_customers_data(customers_file = customers_file)
        transform_product = transfrom_product_data(products_file = product_file)
        merge_output= merget_data_task(transform_sales,transform_customers,transform_product)

    with TaskGroup("analytics") as analytics:

        aggregated_output = aggregated_data_task(merge_output)
        segment_output = segment_customers_task(transform_sales,transform_customers)
        detect_anomalies_output = anomalies_sales_task(transform_sales)
        forecasted_sales_output = forecasted_sales_task(transform_sales)

    with TaskGroup("loading") as loading:
        load_to_snowflake_task.override(task_id="load_cleaned_sales")(transform_sales,
                               config["snowflake"]["database"],
                               config["snowflake"]["targets"]["sales"]["schema"],
                               config["snowflake"]["targets"]["sales"]["table"])
        load_to_snowflake_task.override(task_id="load_cleaned_customers")(transform_customers,
                               config["snowflake"]["database"],
                               config["snowflake"]["targets"]["customers"]["schema"],
                               config["snowflake"]["targets"]["customers"]["table"])
        load_to_snowflake_task.override(task_id="load_cleaned_products")(transform_product,
                               config["snowflake"]["database"],
                               config["snowflake"]["targets"]["products"]["schema"],
                               config["snowflake"]["targets"]["products"]["table"])

        load_to_snowflake_task.override(task_id="load_monthly_sales")(aggregated_output,
                               config["snowflake"]["database"],
                               config["snowflake"]["targets"]["monthly_sales"]["schema"],
                               config["snowflake"]["targets"]["monthly_sales"]["table"])

        load_to_snowflake_task.override(task_id="load_cleaned_sales")(segment_output,
                               config["snowflake"]["database"],
                               config["snowflake"]["targets"]["customer_segment"]["schema"],
                               config["snowflake"]["targets"]["customer_segment"]["table"])

        load_to_snowflake_task.override(task_id="load_detect_sales_anomalies")(detect_anomalies_output,
                               config["snowflake"]["database"],
                               config["snowflake"]["targets"]["detect_sales_anomalies"]["schema"],
                               config["snowflake"]["targets"]["detect_sales_anomalies"]["table"])

        load_to_snowflake_task.override(task_id="load_forecast_sales")(forecasted_sales_output,
                               config["snowflake"]["database"],
                               config["snowflake"]["targets"]["forecast_sales"]["schema"],
                               config["snowflake"]["targets"]["forecast_sales"]["table"])

etl_pipeline_dag()
