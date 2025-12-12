from sqlalchemy import create_engine
import pandas as pd
import boto3
import io

from etl_process import df_data_raw

#A.create client
s3 = boto3.client(
    's3',
        aws_access_key_id = "...",
        aws_secret_access_key = "..." )

# 1.	Extract Data From CSV File
# Goal: Read raw data into a Python DataFrame.
#B.define path
BUCKET_NAME = "course-data-warehouse-viki"
FOLDER_NAME ="ETLANDELT/"
FILE_NAME = "sales_data.csv"

FULL_PATH = FOLDER_NAME + FILE_NAME
#C.Read
def read_files(bucket_name: str, full_path: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket_name, Key=full_path) #get_object - gets an object from s3
    df_raw_data = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df_raw_data

# 3.	Transform the Data
# Goal: Clean, normalize, and enhance the data for better usability
def transformation(df_raw_data: pd.DataFrame) -> pd.DataFrame:
    df_raw_data.columns = df_raw_data.columns.str.lower().str.replace(" ", "_")
    df_raw_data.rename(columns={"diskount": "discount"}, inplace=True)
    df_raw_data["order_date"] = pd.to_datetime(df_raw_data["order_date"], format="%d-%m-%y").fillna('2023-01-01')
    df_raw_data["amount"] = df_raw_data["amount"].astype(float).fillna(0)
    df_raw_data["total_amount"] = (df_raw_data["amount"] * df_raw_data["quantity"]) * (1 - df_raw_data["discount"] / 100)
    df_raw_data["total_amount"] = df_raw_data["total_amount"].apply(lambda x: f"{x:.2f}")
    return df_raw_data


# 4.	Load the Data into SQLite
# Goal: Save the cleaned data into a PostgresSQL database for storage and analysis.
def load_to_postgresql(df: pd.DataFrame, table_name: str) -> None:
    user = "postgres"
    password = "...."
    host = "localhost"
    port = "5432"
    database = "sales_data"

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data loaded successfully into table '{table_name}'.")

df_row_data= read_files(bucket_name= BUCKET_NAME,full_path= FULL_PATH)
df_transformed_data = transformation(df_row_data)
load_to_postgresql(df_transformed_data, "sales_data")

