import pandas as pd

from include.etl.extract_data import get_storage_option


def load_df_to_s3_csv(df: pd.DataFrame, s3_path:str,aws_conn_id:str):
    s3_hook, storage_options = get_storage_option(aws_conn_id)

    try:
        df.to_csv(s3_path, index=False, storage_options=storage_options)
    except Exception as err:
        print(f"Error uploading vsv to S3 {s3_path}: {err}")

    print(f'Finished uploading CSV to S3 {s3_path}')