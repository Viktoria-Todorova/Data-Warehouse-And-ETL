CREATE SCHEMA staging_layer;

CREATE TABLE staging_layer.staging_sales (
    customer_id VARCHAR NOT NULL,
    customer_name VARCHAR,
    product_id VARCHAR NOT NULL,
    product_name VARCHAR,
    sales_amount NUMERIC CHECK (sales_amount >= 0),
    quantity INT CHECK (quantity > 0),
    transaction_date DATE NOT NULL
);

CREATE SCHEMA storage_layer;
-- 2.Create the core layer table
CREATE TABLE storage_layer.core_sales AS
SELECT
    customer_id,
    product_id,
    transaction_date,
    SUM(sales_amount) AS total_sales
FROM staging_layer.staging_sales
GROUP BY customer_id, product_id, transaction_date;

-- 3 Create Dimensional Table for date
CREATE TABLE storage_layer.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day VARCHAR NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL
);
--Insert data
INSERT INTO storage_layer.dim_date(date_key, full_date, day, month, month_name, quarter, year)
SELECT DISTINCT
	TO_CHAR(transaction_date, 'YYYYMMDD')::INT AS date_key,
	transaction_date AS full_date,
	TRIM(TO_CHAR(transaction_date, 'Day')) AS day,
	EXTRACT(MONTH FROM transaction_date)::INT AS month,
	TRIM(TO_CHAR(transaction_date, 'Month')) AS month_name,
	EXTRACT(QUARTER FROM transaction_date)::INT AS quarter,
	EXTRACT(YEAR FROM transaction_date)::INT AS year
FROM staging_layer.staging_sales;

--Create Dimensional Table for customer
CREATE TABLE storage_layer.dim_customer (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL
);
--Insert data
INSERT INTO storage_layer.dim_customer(customer_id, customer_name)
SELECT DISTINCT customer_id, customer_name
FROM staging_layer.staging_sales;

--Create Dimensional Table for product
CREATE TABLE storage_layer.dim_product (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL
);
--Insert data in product
INSERT INTO storage_layer.dim_product(product_id, product_name)
SELECT DISTINCT product_id, product_name
FROM staging_layer.staging_sales;


-- 4.Create Fact Table for sales transactions
CREATE TABLE storage_layer.fact_sales (
    sales_id SERIAL PRIMARY KEY,
    customer_id VARCHAR NOT NULL REFERENCES storage_layer.dim_customer(customer_id),
    product_id VARCHAR NOT NULL REFERENCES storage_layer.dim_product(product_id),
    sales_amount NUMERIC CHECK (sales_amount >= 0),
    transaction_date_key INT NOT NULL REFERENCES storage_layer.dim_date(date_key)
);

-- 5.Insert cleaned data into Fact Table
INSERT INTO storage_layer.fact_sales (customer_id, product_id, sales_amount, transaction_date_key)
SELECT
    customer_id,
    product_id,
    total_sales,
    TO_CHAR(transaction_date, 'YYYYMMDD')::INT -- Convert date into date_key
FROM storage_layer.core_sales;


CREATE SCHEMA datamart;
--6. Create a denormalized view for BI tools
-- SAVES THE FRESH DATA
CREATE OR REPLACE VIEW datamart.sales_dashboard AS
SELECT
    c.customer_name,
    p.product_name,
    f.sales_amount,
    d.full_date,
    d.month_name,
    d.year
FROM storage_layer.fact_sales  f
JOIN storage_layer.dim_customer c ON f.customer_id = c.customer_id
JOIN storage_layer.dim_product p ON f.product_id = p.product_id
JOIN storage_layer.dim_date d ON f.transaction_date_key = d.date_key;

--When we have more serious queriesand we don't need the data
CREATE MATERIALIZED VIEW datamart.sales_dashboard_mv AS
SELECT
    c.customer_name,
    p.product_name,
    d.full_date,
    d.month_name,
    d.year,
    SUM(f.sales_amount) AS total_sales
FROM storage_layer.fact_sales f
JOIN storage_layer.dim_customer c ON f.customer_id = c.customer_id
JOIN storage_layer.dim_product p ON f.product_id = p.product_id
JOIN storage_layer.dim_date d ON f.transaction_date_key = d.date_key
GROUP BY c.customer_name, p.product_name, d.full_date, d.month_name, d.year;

REFRESH MATERIALIZED VIEW datamart.sales_dashboard_mv;
