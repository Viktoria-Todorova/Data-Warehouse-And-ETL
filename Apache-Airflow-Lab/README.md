
This exercise walks you through creating an ETL pipeline to fetch data from S3, transform it, and load it into a PostgreSQL database using Astronomer (Astro). Astro makes managing Airflow projects simpler and production-ready.

## 1.	Install the Astro CLI:
https://www.astronomer.io/docs/astro/cli/install-cli

## 2.	Create an Astro project:
winget install -e --id Astronomer.Astro

astro dev init

<img width="1844" height="758" alt="image" src="https://github.com/user-attachments/assets/ee5f82ce-5dae-4e78-b73a-c1098fe5fbc7" />

open Pycharm

 pip install pandas numpy apache-airflow requests

### start docker, but dont start the containers ->  astro dev start

 <img width="965" height="77" alt="image" src="https://github.com/user-attachments/assets/c5f9c6a1-9cf1-4d4f-9367-f81e7dd0ab21" />


<img width="1310" height="180" alt="image" src="https://github.com/user-attachments/assets/9505b613-98ef-48c5-a4ce-047fd31897b6" />


it will open 

<img width="1900" height="897" alt="image" src="https://github.com/user-attachments/assets/474340dd-4b0d-4766-83c1-0629fbe137b2" />

## the name of the DAG

<img width="1866" height="368" alt="image" src="https://github.com/user-attachments/assets/86086007-e673-43f8-a565-9b449981d69f" />

### is the name of the function

<img width="1223" height="585" alt="image" src="https://github.com/user-attachments/assets/70cdfe26-7004-4dc7-b6d9-e235d2865592" />


Save the installed packages
 pip freeze > requirements.txt

 S3
 upload the file 

 <img width="1908" height="575" alt="image" src="https://github.com/user-attachments/assets/adbf5b63-1f0e-4623-8f71-b025c5567f74" />

Snowflake

Create DATABASE airflo_dag_db;

-> Create the table to mach the name of the colums ex.

Create TABLE sales_summary(
region STRING,
sales NUMERIC
);

IN the congig.yaml we need account
we take it from snowflake 
<img width="765" height="742" alt="image" src="https://github.com/user-attachments/assets/b61ba3df-a357-4edd-bf7a-a8a6e6a7624d" />




<img width="1096" height="722" alt="image" src="https://github.com/user-attachments/assets/24ad78ca-8640-4c9c-8f49-adec4e84b52a" />

To do the connection in Airflow we go Admin > Connections

connect to snowflake 
<img width="1154" height="868" alt="image" src="https://github.com/user-attachments/assets/924372cf-08c6-417f-8488-fb20abb91ec4" />





# Airflow S3 to Snowflake ETL Pipeline

![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

A production-ready Apache Airflow DAG that extracts sales data from AWS S3, transforms it by aggregating sales by region, and loads the results into Snowflake.

## 📋 Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Airflow Connections Setup](#airflow-connections-setup)
- [Snowflake Setup](#snowflake-setup)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)

## ✨ Features

- 📦 Extracts CSV data from AWS S3
- 🔄 Transforms data with pandas (column normalization, aggregation)
- 📊 Loads aggregated results into Snowflake
- 🔒 Secure credential management via Airflow connections
- 📝 Comprehensive logging and error handling
- ✅ Data validation and verification steps

## 🏗️ Architecture

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐
│   AWS S3    │──────▶│   Airflow    │──────▶│  Snowflake   │
│  (Source)   │      │ (Transform)  │      │ (Destination)│
└─────────────┘      └──────────────┘      └──────────────┘
```

**Pipeline Steps:**
1. **Extract**: Read CSV file from S3 bucket
2. **Transform**: Lowercase columns, convert types, aggregate by region
3. **Load**: Insert transformed data into Snowflake table

## 📦 Prerequisites

- Apache Airflow 2.x+
- Python 3.8+
- AWS Account with S3 access
- Snowflake Account
- Required Python packages:
  ```
  apache-airflow-providers-amazon
  apache-airflow-providers-snowflake
  pandas
  boto3
  snowflake-connector-python
  ```

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/airflow-s3-snowflake-etl.git
cd airflow-s3-snowflake-etl
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Copy DAG to Airflow

```bash
cp exampledag.py $AIRFLOW_HOME/dags/
cp -r include $AIRFLOW_HOME/
```

## ⚙️ Configuration

### Create `include/config.yaml`

```yaml
aws_conn_id: aws_conn_id
s3:
  bucket: your-bucket-name
  folder: path/to/your/file.csv
snowflake:
  conn_id: my_snowflake_conn
  account: YOUR_ACCOUNT_ID
  warehouse: COMPUTE_WH
  database: AIRFLOW_DAG_DB
  schema: PUBLIC
  table: sales_summary
```

**Configuration Parameters:**

| Parameter | Description | Example |
|-----------|-------------|---------|
| `aws_conn_id` | Airflow AWS connection ID | `aws_conn_id` |
| `s3.bucket` | S3 bucket name | `my-data-bucket` |
| `s3.folder` | Path to CSV file in S3 | `data/sales.csv` |
| `snowflake.conn_id` | Airflow Snowflake connection ID | `my_snowflake_conn` |
| `snowflake.account` | Snowflake account identifier | `ABC12345.us-east-1` |
| `snowflake.warehouse` | Snowflake warehouse name | `COMPUTE_WH` |
| `snowflake.database` | Target database | `AIRFLOW_DAG_DB` |
| `snowflake.schema` | Target schema | `PUBLIC` |
| `snowflake.table` | Target table name | `sales_summary` |

## 🔐 Airflow Connections Setup

### AWS Connection

1. Navigate to **Airflow UI** → **Admin** → **Connections**
2. Click **+** to add a new connection
3. Fill in the following:

| Field | Value |
|-------|-------|
| Connection Id | `aws_conn_id` |
| Connection Type | `Amazon Web Services` |
| AWS Access Key ID | `your-access-key-id` |
| AWS Secret Access Key | `your-secret-access-key` |
| Extra | `{"region_name": "us-east-1"}` (optional) |

4. Click **Save**

### Snowflake Connection

1. Navigate to **Airflow UI** → **Admin** → **Connections**
2. Click **+** to add a new connection
3. Fill in the following:

| Field | Value |
|-------|-------|
| Connection Id | `my_snowflake_conn` |
| Connection Type | `Snowflake` |
| Account | `YOUR_ACCOUNT_ID` |
| Login | `your_username` |
| Password | `your_password` |
| Warehouse | `COMPUTE_WH` |
| Database | `AIRFLOW_DAG_DB` |
| Schema | `PUBLIC` |
| Role | `ACCOUNTADMIN` |

**Alternative: Using Extra JSON**

```json
{
  "account": "YOUR_ACCOUNT_ID",
  "warehouse": "COMPUTE_WH",
  "database": "AIRFLOW_DAG_DB",
  "role": "ACCOUNTADMIN",
  "schema": "PUBLIC"
}
```

4. Click **Save**

## 🗄️ Snowflake Setup

### Create Database and Table

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS AIRFLOW_DAG_DB;

-- Use the database
USE DATABASE AIRFLOW_DAG_DB;
USE SCHEMA PUBLIC;

-- Create table
CREATE TABLE IF NOT EXISTS sales_summary (
    region VARCHAR(255),
    sales FLOAT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verify table exists
SHOW TABLES LIKE 'SALES_SUMMARY';
```

### Grant Permissions (if not using ACCOUNTADMIN)

```sql
-- Grant database access
GRANT USAGE ON DATABASE AIRFLOW_DAG_DB TO ROLE <your_role>;

-- Grant schema access
GRANT USAGE ON SCHEMA AIRFLOW_DAG_DB.PUBLIC TO ROLE <your_role>;

-- Grant table permissions
GRANT SELECT, INSERT ON TABLE AIRFLOW_DAG_DB.PUBLIC.SALES_SUMMARY TO ROLE <your_role>;
```

## 📖 Usage

### Running the DAG

1. **Enable the DAG** in Airflow UI
2. **Trigger manually** or wait for scheduled run
3. **Monitor progress** in the Graph or Grid view

### Manual Trigger

```bash
# Via Airflow CLI
airflow dags trigger example_astronauts

# Or use the UI: DAGs → example_astronauts → Play button
```

### Verify Results in Snowflake

```sql
USE DATABASE AIRFLOW_DAG_DB;
USE SCHEMA PUBLIC;

-- Check loaded data
SELECT * FROM sales_summary ORDER BY region;

-- Check row count
SELECT COUNT(*) FROM sales_summary;

-- View total sales by region
SELECT region, sales, loaded_at 
FROM sales_summary 
ORDER BY sales DESC;
```

## 🔍 Troubleshooting

### Common Issues

<details>
<summary><b>❌ NoCredentialsError: Unable to locate credentials</b></summary>

**Cause**: AWS connection not configured or incorrect

**Solution**:
- Verify AWS connection exists in Airflow UI
- Check Access Key ID and Secret Access Key are correct
- Ensure connection ID matches config.yaml (`aws_conn_id`)
</details>

<details>
<summary><b>❌ DatabaseError: Incorrect username or password</b></summary>

**Cause**: Snowflake credentials are incorrect

**Solution**:
- Double-check Snowflake username and password
- Verify you can log into Snowflake web UI with same credentials
- Check account identifier format (should be `ACCOUNT_ID.REGION`)
</details>

<details>
<summary><b>❌ ProgrammingError: Table does not exist or not authorized</b></summary>

**Cause**: Database name mismatch or missing permissions

**Solution**:
- Verify database name in config.yaml matches Snowflake
- Common typo: `AIRFLO_DAG_DB` vs `AIRFLOW_DAG_DB`
- Ensure table exists: `SHOW TABLES LIKE 'SALES_SUMMARY'`
- Grant necessary permissions (see Snowflake Setup section)
</details>

<details>
<summary><b>❌ Free trial has ended / Warehouse suspended</b></summary>

**Cause**: Snowflake trial expired

**Solution**:
- Add billing information in Snowflake
- Or use alter
