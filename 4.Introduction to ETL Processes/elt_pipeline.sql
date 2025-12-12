---ELT in Snowflake

---CREATE DATABASE
CREATE DATABASE SALES_DATA_DB;
---CREATE RAW SCHEMA
CREATE SCHEMA raw_layer;
--CREATE STAGING SCHEMA
	CREATE SCHEMA staging_layer;
--CREATE AWS_STAGE
CREATE OR REPLACE STAGE staging_layer .aws_stage
    	url = 's3://course-data-warehouse-viki/ETLANDELT/'
    	credentials=(aws_key_id=' aws_key_id ' aws_secret_key= ‘aws_secret_key ');' ||

---CREATE CSV FILE FORMAT
	CREATE OR REPLACE FILE FORMAT staging_layer.csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
--CREATE TABLE SALES_DATA
CREATE OR REPLACE TABLE raw_layer.sales_data(
    order_id VARCHAR PRIMARY KEY,
    customer_id INT,
    order_date STRING,
    amount NUMERIC,
    quantity INT,
    product_id INT,
    discount NUMERIC,
    profit NUMERIC,
    shipping_days INT
);
---LOAD DATA INTO SALES_DATA TABLE
COPY INTO raw_layer.sales_data
FROM @staging_layer.aws_stage
FILE_FORMAT = staging_layer.csv_format
FILES = ('sales_data.csv')
ON_ERROR = 'CONTINUE';


---Create a VIEW:
-- Normalize order_date.
-- Filter transactions where amount > 50.
-- CREATE CLEANSED SCHEMA
CREATE SCHEMA cleansed_layer;

CREATE OR REPLACE VIEW cleansed_layer.cleaned_data AS
WITH CLEANED_DATA AS (
    SELECT
        TRY_TO_DATE(CAST(order_date AS VARCHAR), 'DD-MM-YY') AS order_date,
        amount,
        quantity,
        discount,
        TO_CHAR(
            amount * quantity * (1 - discount / 100),
            'FM9999990.00'
        ) AS total_amount
    FROM raw_layer.sales_data
)
SELECT *
FROM CLEANED_DATA
WHERE total_amount::FLOAT > 50;

-- 7.	Aggregate Data
-- Calculate monthly sales totals
-- CREATE PRESENTATION SCHEMA
CREATE SCHEMA presenattion_layer;

CREATE OR REPLACE VIEW presenattion_layer.monthly_sales AS
SELECT
    DATE_TRUNC('month', order_date) AS sales_month,
    SUM(total_amount::FLOAT) AS total_sales
FROM cleansed_layer.cleaned_data
GROUP BY sales_month;


