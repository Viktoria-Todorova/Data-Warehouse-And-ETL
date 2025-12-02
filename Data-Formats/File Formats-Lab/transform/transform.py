import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower().str.replace(' ', '_')


    df["order_date"] = pd.to_datetime(df["order_date"], format="%d-%m-%y", errors="coerce")
    df["order_id"] = df["order_id"].fillna(-1).astype(int)
    df["customer_id"] = df["customer_id"].fillna(-1).astype(int)
    df["amount"] = df["amount"].fillna(0.0)
    df["quantity"] = df["quantity"].fillna(0).astype(int)
    df["total_revenue"] = df["amount"] * df["quantity"]
    return df
