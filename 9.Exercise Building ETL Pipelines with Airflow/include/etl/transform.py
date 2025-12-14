import pandas as pd
from ..logger import setup_logger
from ..validations.aggregates_schema import validate_pre_aggregate_schema, validate_post_aggregate_schema
from ..validations.anomalies_schema import validate_post_anomalies_schema
from ..validations.customers_shema import validate_pre_customer_schema, validate_post_customer_schema
from ..validations.forecast_schema import validate_post_forecasted_sales_schema
from ..validations.products_schema import validate_post_product_schema
from ..validations.sales_schema import validate_pre_sales_schema, validate_post_sales_schema
from ..validations.segmented_schema import validate_post_segmentation_shema

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

#Segment customers into different categories based on their total spending.
def segment_customers(sales_df:pd.DataFrame, customers_df:pd.DataFrame) -> pd.DataFrame:
    """ Segments customers based on their total spent"""
    logging.info(f"segmenting customers based on their total spent")
    total_spend_df = sales_df.groupby("customer_id")["total_revenue"].sum().reset_index().copy()

    total_spend_df.rename(columns={"total_revenue": "total_spent"}, inplace=True)

    segmented_df = customers_df.merge(total_spend_df, on="customer_id",how="left").copy()

    segmented_df.dropna(subset=["total_spent"], inplace=True)

    segmented_df["customer_segment"] = pd.cut(
        segmented_df["total_spent"],
        bins=[0,1000,5000,10000,float("inf")],
        labels=["Low", "Medium", "High","VIP"],
    )
    segmented_df["segmentation_date"] = pd.to_datetime(customers_df["signup_date"],format="mixed",errors="coerce")
    allowed_columns = ["customer_id", "total_spent","customer_segment","segmentation_date"]
    df_segmented = drop_extra_columns(segmented_df, allowed_columns)
    df_segmented = validate_post_segmentation_shema(df_segmented)
    logging.info(f"Final segmented customers: {len(df_segmented)} rows")
    return df_segmented


#Identify high-value transactions that might be anomalies within the sales data.
def detect_sales_anomalies(sales_df:pd.DataFrame) -> pd.DataFrame:
    logging.info(f"detecting sales anomalies")
    threshold =sales_df["total_revenue"].mean() + (3+ sales_df["total_revenue"].std())
    anomalies_df=sales_df[sales_df["total_revenue"]>threshold].copy() #we use copy when we dont use all the data

    anomalies_df["order_date"]=pd.to_datetime(anomalies_df["order_date"],format="mixed",errors="coerce")
    allowed_columns = [ "order_id","customer_id","product_id","order_date","total_revenue"]
    df_anomalies = drop_extra_columns(anomalies_df, allowed_columns)
    df_anomalies = validate_post_anomalies_schema(df_anomalies)
    logging.info(f"Final anomalies: {len(df_anomalies)} rows")
    return df_anomalies

def forecast_sales(sales_df:pd.DataFrame) -> pd.DataFrame:
    """Sles forecast for 7 days"""
    logging.info(f"forecasting sales")
    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"],format="mixed",errors="coerce")
    sales_df.set_index("order_date", inplace=True) #after that the function we will use will work only on index

    sales_df["sales_forecast"] = sales_df["total_revenue"].rolling(window=7,min_periods=1).mean()
    sales_df.reset_index(inplace=True)

    allowed_columns = ["order_date", "total_revenue", "sales_forecast"]
    sales_df = drop_extra_columns(sales_df, allowed_columns)
    sales_df= validate_post_forecasted_sales_schema(sales_df)
    logging.info(f"Final sales forecast: {len(sales_df)} rows")
    return sales_df

def drop_extra_columns(df: pd.DataFrame, allowed_columns:list) -> pd.DataFrame:
    return df.loc[:, df.columns.isin(allowed_columns)].copy()