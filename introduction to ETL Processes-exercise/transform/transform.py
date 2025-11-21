from importlib.metadata import requires
from random import choices
import numpy as np
import pandas as pd

def clean_data(dfs:list[pd.DataFrame],
               old_columns:str | None,
               new_column_name: str | None) ->list[pd.DataFrame]:
    possible_date_colums = ["signump_date","order_date","delivery_date"]
    cleaned_dfs = []

    for i,df in enumerate(dfs):
        if df is None or df.empty:
            print(f"skipping empty DataFrame at index {i}")
            continue

        df.columns = df.columns.str.lower().str.replace(" ", "_")

        if old_columns in df.columns:
            df.rename(columns={old_columns: new_column_name}, inplace=True)
            print(f"Renamed column {new_column_name} from {old_columns} -> {new_column_name}")


        for col in possible_date_colums:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col],format = "%d-%m-%Y",errors="coerce")

        cleaned_dfs.append(df)
    return cleaned_dfs


def remove_duplicates(dfs:list[pd.DataFrame],
                      subset:list[str] | None = None,
                      keep:str ="first") -> list[pd.DataFrame]:

    cleaned_dfs = []
    for i, df in enumerate(dfs):
        if df.empty or df is None:
            print(f"skipping empty DataFrame at index {i}")
            continue
        before =len(df)
        df = df.drop_duplicates(subset=subset,keep=keep).reset_index(drop=True)

        print(f"Removing {before - len(df)} duplicates at index {i}")
        cleaned_dfs.append(df)

    return cleaned_dfs

def merge_data(dfs:list[pd.DataFrame], merge_column: list[tuple[str,str]], how:str = "inner") -> list[pd.DataFrame]:


    if not dfs or len(dfs) < 2:
        raise ValueError("Need at least 2 DataFrames to merge")
    merged_df = dfs[0]

    for i,df in enumerate(dfs[1:]):
        left_key,right_key = merge_column[i]
        print(f"Merging {left_key} -> {right_key}")
        merged_df = merged_df.merge(df,left_on=left_key,right_on=right_key,how=how)


    return merged_df


def compute_derived_columns(merged_df:pd.DataFrame,) -> pd.DataFrame:
    required_columns = {"amount", "quantity","profit", "discount"}

    missing = required_columns - set(merged_df.columns)
    if missing:
        raise KeyError(f"Missing columns for computation : {missing}")

    merged_df["total_revenue"] = merged_df["amount"]*merged_df["quantity"]
    merged_df["profit_margin"] = merged_df["profit"]/merged_df["total_revenue"]
    merged_df["discounted_price"] = merged_df["amount"] * (1 - merged_df["discount"] / 100)
    return merged_df


def segment_deliveries(merged_df:pd.DataFrame, fast_threshold : int =3, slow_threshold: int =10) -> pd.DataFrame:
    if "shipping_days" not in merged_df.columns:
        raise KeyError("Missing column shipping_days in merged data.")

    conditions = [
        merged_df["shipping_days"] < fast_threshold,
        merged_df["shipping_days"] > slow_threshold,
    ]

    choices = ["fast", "slow"]
    merged_df["delivery_category"] =np.select(conditions, choices, default= "normal")
    return merged_df

def categorize_products(merged_df:pd.DataFrame) -> pd.DataFrame:
    if "amount" not in merged_df.columns:
        raise KeyError("Missing column amount in merged data.")

    bins = [0,50,200,float("inf")]
    labels =["Low","Medium","High"]
    merged_df["price_category"] = pd.cut(merged_df["amount"], bins=bins, labels=labels)
    return merged_df