import pandas as pd
from ..logger import setup_logger
from ..validations.aggregates_schema import validate_pre_aggregate_schema, validate_post_aggregate_schema
from ..validations.customers_shema import validate_pre_customer_schema, validate_post_customer_schema
from ..validations.products_schema import validate_post_product_schema
from ..validations.sales_schema import validate_pre_sales_schema, validate_post_sales_schema

logging = setup_logger("etl.transform")


def clean_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Cleaning sales data from {len(sales_df)} records")
    sales_df= validate_pre_sales_schema(sales_df)
    sales_df.columns=sales_df.columns.str.lower().str.replace(" ", "_")
    #Clean the Data: Remove missing values, standardize columns.
    sales_df.dropna(inplace=True)
    #Format Data: Convert dates to the correct format.
    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"],format="mixed",errors="coerce")
    #Enhance Data: Compute additional columns (total revenue).
    sales_df["total_revenue"] = sales_df["amount"] * sales_df["quantity"]
    sales_df = validate_post_sales_schema(sales_df)
    logging.info(f"cleaned sales data from {len(sales_df)} records")
    return sales_df

def clean_customers_data(customers_df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Cleaning customer data from {len(customers_df)} records")
    customers_df = validate_pre_customer_schema(customers_df)
    customers_df.columns=customers_df.columns.str.lower().str.replace(" ", "_")
    #Clean the Data: Remove missing values, standardize columns.
    customers_df.dropna(inplace=True)
    # Format Data: Convert dates to the correct format.
    customers_df["signup_date"] = pd.to_datetime(customers_df["signup_date"], format="mixed", errors="coerce")

    customers_df = validate_post_customer_schema(customers_df)
    logging.info(f"cleaned customer data from {len(customers_df)} records")
    return customers_df


def clean_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Cleaning product data from {len(products_df)} records")

    products_df.columns=products_df.columns.str.lower().str.replace(" ", "_")
    products_df = validate_post_product_schema(products_df)
    #Clean the Data: Remove missing values, standardize columns.
    products_df.dropna(inplace=True)

    products_df = validate_post_product_schema(products_df)

    logging.info(f"cleaned products data from {len(products_df)} records")
    return products_df

#Merge Data: Combine different datasets to prepare them for analysis.Compute an extra field like profit_margin (e.g., profit divided by amount).
def merge_data(sales_df: pd.DataFrame, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """"
    MERGE SALES DATA, CUSTOMERS DATA, PRODUCTS DATA
    """

    logging.info(f"merging sales, customers and products data")


    merged_df = sales_df.merge(customers_df, on="customer_id", how = "inner").copy()
    merged_df = merged_df.merge(products_df, on="product_id", how = "inner").copy()

    merged_df["profit_margin"] = merged_df["profit"]/merged_df["total_revenue"]

    logging.info(f"merging sales, customers and products data")
    return merged_df

#Develop another task that computes monthly aggregates (total sales and unique customers) using time series grouping (e.g., grouping by order_date with monthly frequency).

def compute_monthly_aggregates(merged_df: pd.DataFrame) -> pd.DataFrame:

    """"
    Agregated data by a month"""
    logging.info(f"computing monthly aggregates")


    merged_df["order_date"]=pd.to_datetime(merged_df["order_date"],format="mixed",errors="coerce")

    merged_df = validate_pre_aggregate_schema(merged_df)

    aggregate_df = merged_df.groupby(pd.Grouper(key="order_date", freq="M")).agg(
        total_sales = ("total_revenue", "sum"),
        unique_customers = ("customer_id", "nunique"),
    ).reset_index().copy()
    aggregate_df = validate_post_aggregate_schema(aggregate_df)
    logging.info(f"aggregating monthly aggregates")
    return aggregate_df