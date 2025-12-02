from config.settings import BUCKET_NAME, FOLDER_PREFIX, FULL_CSV_PATH, FULL_JSON_PATH, FULL_PARQUET_PATH, USER, \
    PASSWORD, HOST, PORT, DATABASE
from extract.extract_data import extract_csv, extract_json, extract_parquet, extract_db_data
from transform.transform import transform_data
import pandera as pa
from validations.pandera_validation import validate_sales_data
from load.load_to_postgres import load_to_postgresql

if __name__ == '__main__':
    sales_df_csv = extract_csv(bucket=BUCKET_NAME,
                               full_path = FOLDER_PREFIX + FULL_CSV_PATH)
    sales_df_json = extract_json(bucket=BUCKET_NAME,
                                 full_path = FOLDER_PREFIX + FULL_JSON_PATH)

    sales_df_parquet = extract_parquet(bucket=BUCKET_NAME,
                                        full_path = FOLDER_PREFIX + FULL_PARQUET_PATH)

    sakes_db_data= extract_db_data(db_url=f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

    transform_csv_df = transform_data(sales_df_csv)
    transform_parquet_df =transform_data(sales_df_parquet)

    try:
        valid_df_csv = validate_sales_data(sales_df_csv)
        print("DataFrame(CSV) is valid")
    except pa.errors.SchemaError as e:
        raise ValueError(f"DataFrame validation failed: {e}")

    try:
        valid_parquet_csv = validate_sales_data(sales_df_parquet)
        print("DataFrame(Parquet) is valid")
    except pa.errors.SchemaError as e:
        raise ValueError(f"DataFrame validation failed: {e}")

    # print(sakes_db_data)
    load_to_postgresql(valid_df_csv,"valid_csv_data",f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    load_to_postgresql(valid_parquet_csv, "valid_parquet_data",
                       f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

