# import boto3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

aws_access_key_id ="..."
aws_secret_access_key ="..."


BUCKET_NAME = 'course-data-warehouse-viki'

FULL_CSV_PATH ='DataFormats/sales_data.csv'
FULL_PARQUET_PATH ='DataFormats/sales_data.parquet'

USER = 'postgres'
PASSWORD = '...'
HOST='localhost'
PORT='5432'
DATABASE='sales_data_new'