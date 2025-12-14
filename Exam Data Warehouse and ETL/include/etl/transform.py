import pandas as pd

from include.validations.product_schema_validation import validate_input_product_schema, validate_output_product_schema
from include.validations.sales_shema_validation import  validate_output_sales_schema

from ..logger import setup_logger
logging = setup_logger("etl.transform_data")


def transform_sales_data(sales_df:pd.DataFrame) -> pd.DataFrame:

    sales_df.columns = sales_df.columns.str.strip().str.lower().str.replace(' ', '_')
    sales_df = sales_df.rename(columns={
        "qty": "quantity",
        "time_stamp": "timestamp",
    })

    sales_df = sales_df.dropna(subset=["region"])
    sales_df["region"] = sales_df["region"].str.lower()
    sales_df=sales_df.rename(columns={"qty": "quantity"})
    sales_df = sales_df[(sales_df["quantity"] > 0) & (sales_df["price"] > 0)]
    sales_df["timestamp"] = pd.to_datetime(sales_df["timestamp"], format="mixed", errors="coerce")

    return validate_output_sales_schema(sales_df)

def transform_product_data(product_df:pd.DataFrame) -> pd.DataFrame:
    product_df = validate_input_product_schema(product_df)
    product_df.columns = product_df.columns.str.strip().str.replace(' ', '_')
    product_df["category"] = product_df["category"].str.lower()
    product_df["brand"] = product_df["brand"].apply(
        lambda b: b[:-1].lower() + "_" + b[-1].lower()
    )
    product_df = product_df[product_df["rating"] >= 0]
    product_df=  product_df.dropna(subset=["launch_date"])
    product_df["launch_date"] = pd.to_datetime(product_df["launch_date"], format="mixed", errors="coerce")


    product_df = product_df.drop_duplicates()

    return validate_output_product_schema(product_df)

def merge_data(sales_df:pd.DataFrame, product_df:pd.DataFrame) -> pd.DataFrame:
    logging.info(f"merging sales data with products")

    merged_df = sales_df.merge(product_df, on="product_id", how="inner").copy()
    merged_df["total_sales"] = merged_df["quantity"] * merged_df["price"] * (1 - merged_df["discount"].fillna(0))

    merged_df = merged_df.groupby(["category", "timestamp"], as_index=False).agg({
        "quantity": "sum",
        "total_sales": "sum"
    }).rename(columns={"quantity": "total_quantity"})

    logging.info(f"completed merging and aggregating sales data with products")
    return merged_df
