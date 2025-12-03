import pandas as pd

from config.settings import BUCKET_NAME, FULL_CSV_PATH, FULL_PARQUET_PATH, DATABASE, PASSWORD, USER, HOST
from extract.aws_s3_extract import extract_csv_from_s3, extract_parquet_from_s3
from extract.extract_api import extract_weather_from_sinoptik
from extract.extract_data import extract_customers_form_json, extract_and_flatten_orders
from extract.postgres_extract import extract_sales_from_database
from load.load_data_to_s3 import load_df_to_s3_json, load_df_to_s3_csv
from load.load_local_data import load_to_json, load_to_csv
from validations.customer_validation import validate_customers
from validations.orders_validation import validate_orders
from validations.sales_validations import validate_sales
from validations.weather_validations import validate_weather

if __name__ == "__main__":
    customer_df = extract_customers_form_json("../files/customers.json")
    order_df = extract_and_flatten_orders("../files/orders.json")

    order_df["order_id"] =order_df["order_id"].astype(int)
    order_df["customer_id"]= order_df["customer_id"].astype(int)

    merge_df = pd.merge(order_df, customer_df, how="left", on="customer_id")

    sales_df = extract_csv_from_s3(bucket_name=BUCKET_NAME, file_key=FULL_CSV_PATH)

    sales_df_parquet = extract_parquet_from_s3(bucket_name=BUCKET_NAME, file_key=FULL_PARQUET_PATH)

    sql_query = "SELECT * FROM sales_data"
    dp_params ={
        "database": DATABASE,
        "user": USER,
        "password": PASSWORD,
        "host": HOST,
        "port": 5432
    }

    sales_df_db = extract_sales_from_database(sql_query,dp_params)
    weather_db = extract_weather_from_sinoptik("sofia")

    validate_customer_df = validate_customers(customer_df)
    validate_orders_df =validate_orders(order_df)
    validate_sales_df=validate_sales(sales_df)
    validate_weather_df = validate_weather(weather_db)

    load_to_json(sales_df_db, "sales_data_db.json")
    load_to_json(validate_customer_df, "customer_date.json")
    load_to_json(validate_orders_df, "orders_data.json")

    load_to_csv(validate_sales_df, "sales_data.csv")
    load_to_csv(validate_weather_df, "weather_data.csv")


    load_df_to_s3_csv(sales_df, f"s3://{BUCKET_NAME}/test/sales_data.csv")
    load_df_to_s3_csv(order_df, f"s3://{BUCKET_NAME}/test/orders_data.csv")
