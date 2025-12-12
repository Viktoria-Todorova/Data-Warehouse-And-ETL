import pandas as pd

from include.validations.enrich_schema import validate_output_enriched_schema
from include.validations.hourly_sales import validate_output_hourly_sales_trend_schema
from include.validations.product_sales_schema import validate_product_sales_rankicng
from include.validations.product_schema import validate_output_product_schema, validate_input_product_schema
from include.validations.revenue_shema import validate_revenue_concentration_schema
from include.validations.sales_schema import validate_input_sales_schema, validate_output_sales_schema
from include.validations.seasonal_sales_schema import validate_output_seasonal_sales_schema


# PHASE 5: Transformation in Pandas
# Core Tasks in transform.py:
# Sales Data Cleaning And Validate Output Data (the schema must mirror the post-transform “expected” state of each column)
# •	Normalize columns to snake_case format
# •	Normalize region to lowercase, strip whitespace
# •	Drop missing region or timestamp
# •	Remove rows with price <= 0 or quantity <= 0
# •	Convert timestamp to datetime
# •	Recalculate total sales

def transform_sales_data(sales_df:pd.DataFrame) -> pd.DataFrame:
    sales_df = validate_input_sales_schema(sales_df)
    sales_df.columns=sales_df.columns.str.strip().str.replace(' ', '_')
    sales_df["region"]= sales_df["region"].str.strip().str.lower()
    sales_df = sales_df.dropna(subset=["region","timestamp"])
    sales_df =sales_df[(sales_df["price"]>0) &  (sales_df["quantity"]>0)]

    sales_df["timestamp"]=pd.to_datetime(sales_df["timestamp"],format="mixed",errors="coerce")
    sales_df["total_sales"] =sales_df["quantity"]*sales_df["price"]
    return validate_output_sales_schema(sales_df)


# Product Cleaning And Validate Output Data (your schema must mirror the post-transform “expected” state of each column)
# •	Normalize columns to snake_case format
# •	Standardize brand (uppercase) and category (lowercase)
# •	Drop missing product_id, rating
# •	Drop duplicates
def transform_product_data(product_df:pd.DataFrame) -> pd.DataFrame:
    product_df=validate_input_product_schema(product_df)
    product_df.columns = product_df.columns.str.strip().str.replace(' ', '_')
    product_df["brand"]=product_df["brand"].str.strip().str.upper()
    product_df["category"] = product_df["category"].str.strip().str.lower()
    product_df=product_df.dropna(subset=["product_id","rating"])
    product_df=product_df.drop_duplicates()
    return validate_output_product_schema(product_df)
#
# PHASE 6: Validate Output All Data (the schema must mirror the post-transform “expected” state of each column)
# Merge:
# •	Join sales with metadata on product_id
# •	Drop unmatched records
# Enrich:
# •	Create new columns:
# o	month, weekday, hour from timestamp
# o	sales_bucket using pd.cut() on total_sales

def merge_sales_and_product_data(sales_df:pd.DataFrame, product_df:pd.DataFrame) -> pd.DataFrame:
    merge_df =sales_df.merge(product_df, how="inner", on="product_id")
    return merge_df


def enrich_merged_data(merged_df:pd.DataFrame) -> pd.DataFrame:
    merged_df["timestamp"] =pd.to_datetime(merged_df["timestamp"],format="mixed",errors="coerce")
    merged_df["month"] = merged_df["timestamp"].dt.to_period("M").astype(str)
    merged_df["week"] = merged_df["timestamp"].dt.isocalendar().week
    merged_df["day"] = merged_df["timestamp"].dt.day_name()
    merged_df["hour"] = merged_df["timestamp"].dt.hour.astype("int64")
    merged_df["sales_bucket"] = pd.cut(
        merged_df["total_sales"],
        bins =[0,100,500,float("inf")],
        labels=["Low","Medium","High"]
    )

    return validate_output_enriched_schema(merged_df)

# 1. Hourly Sales Trend Analysis by Region And Validate Data (the schema must mirror the post-transform “expected” state of each column)

def hourly_sales_trend(enriched_df:pd.DataFrame) -> pd.DataFrame:
    agg = enriched_df.groupby(["region","category","hour"], as_index = False).agg(hourly_sales_trend=("total_sales","sum"))
    idx =agg.groupby(["region","category"])["hourly_sales_trend"].idxmax()
    peaks = agg.loc[idx].reset_index(drop=True)
    return validate_output_hourly_sales_trend_schema(peaks)

# 2. Product Sales Ranking and Performance Categorization And Validate Data (the schema must mirror the post-transform “expected” state of each column)

def product_sales_ranking_with_brand(enriched_df:pd.DataFrame) -> pd.DataFrame:
    summary =(enriched_df.groupby(["product_id","category","brand","rating"],as_index=False)
              .agg(revenue=("total_sales","sum"),sales_count=("quantity","sum")))

    summary["value_bucket"] =pd.qcut(
        summary["revenue"],
        q=[0,0.2,0.8,1],
        labels = ["Low Performance", "Average","Bestseller"]
    )

    return validate_product_sales_rankicng(summary)

# 3. Seasonal Sales Patterns by Quarter and Category And Validate Data (the schema must mirror the post-transform “expected” state of each column)
# Analyze how sales vary on a quarterly basis and how different product categories perform in each quarter. This task emphasizes the influence of seasonality on revenue and order behavior.

def seasonal_sales_pattern(enriched_df:pd.DataFrame) -> pd.DataFrame:
    enriched_df["timestamp"] = pd.to_datetime(enriched_df["timestamp"],format="mixed",errors="coerce")
    enriched_df["quarter"] =enriched_df["timestamp"].dt.to_period("Q").astype(str)
    seasonal_pattern =enriched_df.groupby(["quarter","category"],as_index=False).agg(total_sales=("total_sales","sum"))

    return validate_output_seasonal_sales_schema(seasonal_pattern)

# 4.Revenue Concentration and Inequality Analysis by Region And Validate Data (the schema must mirror the post-transform “expected” state of each column)
# Examine how revenue is distributed across regions and measure the concentration of sales. This task calculates each
# region’s revenue share and computes a basic inequality metric (e.g., cumulative percentage) that can serve as the basis for more formal measures
# (like the Gini coefficient).

def revenue_concentration_pattern(enriched_df:pd.DataFrame) -> pd.DataFrame:
    summary = enriched_df.groupby(["region"],as_index=False).agg(region_revenue = ("total_sales", "sum"))
    total = summary["region_revenue"].sum()
    summary["revenue_share"] = summary["region_revenue"]/total
    summary["cumulative_share"] = summary["region_revenue"].cumsum()

    return validate_revenue_concentration_schema(summary)