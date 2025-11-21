from config.settings import BUCKET_NAME, FOLDER
from extract.extract_s3 import extract_s3
from load.load_to_postgres import load_to_postgres, load_transformed
from transform.transform import clean_data, remove_duplicates, merge_data, compute_derived_columns, segment_deliveries, \
    categorize_products


def run_etl_pipeline():
    sales_df,customer_df,product_df,shipping_df = extract_s3(bucket_name = BUCKET_NAME,folder=FOLDER)
    cleaned_dfs = clean_data([sales_df,customer_df,product_df,shipping_df], old_columns="diskount", new_column_name="discount")
    deduped_dfs = remove_duplicates(dfs=cleaned_dfs)
    merged_columns= [
        ("customer_id","customer_id"),
        ("product_id","product_id"),
        ("order_id","order_id"),
    ]

    merged_df = merge_data(dfs= deduped_dfs,merge_column=merged_columns)
    merged_df = segment_deliveries(merged_df = merged_df)
    merged_df = compute_derived_columns(merged_df = merged_df)
    merged_df = categorize_products(merged_df = merged_df)
    load_to_postgres(df= merged_df,table_name= "sales_data")
    load_transformed(df= merged_df,table_name= "sales_transformed")

    print(sales_df)

if __name__ == "__main__":
    run_etl_pipeline()