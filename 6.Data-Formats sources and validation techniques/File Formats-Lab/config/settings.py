import boto3

#create s3 client
S3= boto3.client(
    "s3",
    aws_access_key_id =...,
    aws_secret_access_key =...,
)

#define bucket name
BUCKET_NAME = 'course-data-warehouse-viki'
FOLDER_PREFIX = 'ETLANDELT/'
FULL_CSV_PATH ='sales_data.csv'
FULL_PARQUET_PATH ='sales_data.parquet'
FULL_JSON_PATH ='sales_data.json'

USER = 'postgres'
PASSWORD = ...
HOST='localhost'
PORT='5432'
DATABASE='sales_data'