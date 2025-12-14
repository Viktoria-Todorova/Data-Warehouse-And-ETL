# Globo Retail ETL Pipeline Documentation

## Overview
A complete ETL pipeline built with Apache Airflow that extracts retail data from AWS S3, transforms it, and loads it into Snowflake for analytics.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           AIRFLOW ETL PIPELINE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐        │
│  │   EXTRACT    │ ───► │  TRANSFORM   │ ───► │     LOAD     │        │
│  │    (S3)      │      │   (Pandas)   │      │  (Snowflake) │        │
│  └──────────────┘      └──────────────┘      └──────────────┘        │
│         │                     │                      │                 │
│         ▼                     ▼                      ▼                 │
│  • Sales CSV           • Clean data          • CLEANSED_LAYER         │
│  • Products JSON       • Validate            • ANALYTICS_LAYER        │
│                        • Merge                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### 1. **Source Data (S3)**
- **Location**: `s3://course-data-warehouse-viki/InputExam/`
- **Files**:
  - `sales_data.csv` - Sales transactions
  - `product_data.json` - Product catalog

### 2. **Processing (Airflow)**
The pipeline consists of 4 main task groups:

#### **A. Extract Data Group**
```python
extract_data_group()
├── extract_csv_file()      # Get all CSV files from S3
├── extract_json_file()     # Get all JSON files from S3
├── get_sales_path()        # Identify sales data file
└── get_product_path()      # Identify product data file
```

#### **B. Transform Group**
```python
transform_group()
├── transform_sales()       # Clean and validate sales data
│   ├── Remove duplicates
│   ├── Fill missing values
│   ├── Validate datatypes
│   └── Save to S3 as CSV
│
└── transform_products()    # Clean and validate product data
    ├── Remove duplicates
    ├── Fill missing values
    ├── Validate datatypes
    └── Save to S3 as CSV
```

#### **C. Merge Data**
```python
merged_data_task()
├── Read cleaned sales (JSON string from XCom)
├── Read cleaned products (JSON string from XCom)
├── Merge on product_id
├── Calculate total_sales = quantity * price * (1 - discount)
├── Group by category and timestamp
└── Save merged data to S3
```

#### **D. Load Group**
```python
load_group()
├── load_sales         # Load to CLEANSED_LAYER.CLEANED_SALES
├── load_products      # Load to CLEANSED_LAYER.CLEANED_PRODUCTS
└── load_merged        # Load to ANALYTICS_LAYER.PRODUCT_SALES
```

---

## Snowflake Data Warehouse Architecture

### Database Structure
```
AIRFLOW_EXAM_DEC
├── STAGING_LAYER
│   ├── RETAIL_S3_STAGE (External Stage)
│   └── CSV_FORMAT (File Format)
│
├── CLEANSED_LAYER
│   ├── CLEANED_SALES
│   └── CLEANED_PRODUCTS
│
├── BUSINESS_LAYER
│   ├── DIM_DATE (Dimension)
│   ├── DIM_PRODUCT (Dimension)
│   └── FACT_SALES (Fact Table)
│
└── ANALYTICS_LAYER
    ├── PRODUCT_SALES
    ├── MV_SALES_BY_REGION_MONTH
    ├── MV_TOP_PRODUCTS_BY_REVENUE
    ├── MV_REVENUE_TREND
    └── MV_CATEGORY_PERFORMANCE
```

---

## Table Schemas

### CLEANSED_LAYER.CLEANED_SALES
| Column | Type | Description |
|--------|------|-------------|
| sales_id | INT | Unique sale identifier |
| product_id | INT | Product reference |
| region | STRING | Geographic region |
| quantity | INT | Units sold |
| price | NUMERIC(10,2) | Unit price |
| timestamp | DATE | Sale date |
| discount | NUMERIC(10,2) | Discount percentage |
| order_status | STRING | Order status |

### CLEANSED_LAYER.CLEANED_PRODUCTS
| Column | Type | Description |
|--------|------|-------------|
| product_id | INT | Unique product identifier |
| category | STRING | Product category |
| brand | STRING | Brand name |
| rating | NUMERIC(10,2) | Product rating |
| in_stock | BOOLEAN | Availability status |
| launch_date | DATE | Product launch date |

### ANALYTICS_LAYER.PRODUCT_SALES
| Column | Type | Description |
|--------|------|-------------|
| category | STRING | Product category |
| timestamp | DATE | Sale date |
| total_quantity | BIGINT | Total units sold |
| total_sales | FLOAT | Total revenue |

---

## Key Transformations

### Sales Data Transformation
```python
def transform_sales_data(df):
    # Remove duplicates
    df = df.drop_duplicates(subset=['sales_id'])
    
    # Fill missing values
    df['discount'].fillna(0, inplace=True)
    df['order_status'].fillna('pending', inplace=True)
    
    # Convert datatypes
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['price'] = pd.to_numeric(df['price'])
    
    return df
```

### Product Data Transformation
```python
def transform_product_data(df):
    # Remove duplicates
    df = df.drop_duplicates(subset=['product_id'])
    
    # Fill missing values
    df['rating'].fillna(0.0, inplace=True)
    df['in_stock'].fillna(False, inplace=True)
    
    # Convert datatypes
    df['launch_date'] = pd.to_datetime(df['launch_date'])
    
    return df
```

### Merge Logic
```python
def merge_data(sales_df, product_df):
    # Inner join on product_id
    merged_df = sales_df.merge(product_df, on="product_id", how="inner")
    
    # Calculate total_sales with discount
    merged_df["total_sales"] = (
        merged_df["quantity"] * 
        merged_df["price"] * 
        (1 - merged_df["discount"].fillna(0))
    )
    
    # Aggregate by category and timestamp
    result_df = merged_df.groupby(
        ["category", "timestamp"], 
        as_index=False
    ).agg({
        "quantity": "sum",
        "total_sales": "sum"
    }).rename(columns={"quantity": "total_quantity"})
    
    return result_df
```

---

## DAG Configuration

### Task Dependencies
```
debug_s3_access
    ↓
extract_data_group
    ↓
transform_group
    ↓
merged_data_task
    ↓
load_group (parallel)
    ├── load_sales
    ├── load_products
    └── load_merged
```

### Configuration File (config.yaml)
```yaml
aws_conn_id: "aws_conn_id"

s3:
  bucket: "course-data-warehouse-viki"
  folder: "InputExam"
  output_folder: "OutputExam"

snowflake:
  database: "AIRFLOW_EXAM_DEC"
  targets:
    sales:
      schema: "CLEANSED_LAYER"
      table: "CLEANED_SALES"
    products:
      schema: "CLEANSED_LAYER"
      table: "CLEANED_PRODUCTS"
    merged:
      schema: "ANALYTICS_LAYER"
      table: "PRODUCT_SALES"
```

---

## Important Implementation Details

### 1. XCom Data Passing
- Cleaned DataFrames are serialized as JSON strings using `orient="split"`
- JSON strings are passed between tasks via XCom
- StringIO is used when reading JSON strings to avoid deprecation warnings

```python
# Serializing
return df.to_json(orient="split")

# Deserializing
df = pd.read_json(StringIO(json_string), orient="split")
```

### 2. AWS S3 Authentication
```python
# Get credentials from Airflow connection
aws_conn = BaseHook.get_connection('aws_conn_id')
storage_options = {
    'key': aws_conn.login,
    'secret': aws_conn.password,
}

# Use with pandas
df = pd.read_csv(s3_path, storage_options=storage_options)
```

### 3. Snowflake Loading
```python
def load_data_to_snowflake(df, database, schema, table):
    snowflake_hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    engine = snowflake_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        conn.execute(f"USE DATABASE {database}")
        conn.execute(f"USE SCHEMA {schema}")
        conn.execute(f"TRUNCATE TABLE IF EXISTS {table}")
        
        df.to_sql(
            name=table,
            con=conn,
            schema=schema,
            index=False,
            if_exists='append',
            method="multi",
            chunksize=10000
        )
```

---

## Materialized Views (Analytics Layer)

### MV_SALES_BY_REGION_MONTH
Aggregates sales by region and month for regional performance analysis.

### MV_TOP_PRODUCTS_BY_REVENUE
Categorizes products by revenue tiers:
- TOP REVENUE: > $25,000
- MIDDLE REVENUE: $15,000 - $25,000
- LOW REVENUE: < $15,000

### MV_REVENUE_TREND
Tracks revenue trends over time by year and month.

### MV_CATEGORY_PERFORMANCE
Analyzes category-level performance with quantity and sales metrics.

---

## Error Handling

### Common Issues and Solutions

1. **AWS Credentials Error**
   - Ensure `aws_conn_id` connection is configured in Airflow
   - Use `storage_options` when reading from S3

2. **Snowflake Permission Error**
   - Grant necessary permissions to the role:
   ```sql
   GRANT CREATE TABLE ON SCHEMA AIRFLOW_EXAM_DEC.ANALYTICS_LAYER TO ROLE data_engineer;
   GRANT USAGE ON DATABASE AIRFLOW_EXAM_DEC TO ROLE data_engineer;
   ```

3. **JSON Parsing Warning**
   - Always wrap JSON strings in `StringIO()` when reading
   - Match `orient` parameter between `to_json()` and `read_json()`

---

## Monitoring and Logging

All tasks include comprehensive logging:
- Task start/end times
- Row counts at each stage
- Error messages with context
- Successful load confirmations

Example log output:
```
[2025-12-14 02:01:33] INFO - Successfully loaded 2375 rows to AIRFLOW_EXAM_DEC.ANALYTICS_LAYER.product_sales
```

---

## Performance Considerations

1. **Chunked Loading**: Uses `chunksize=10000` for large datasets
2. **Parallel Loading**: Load tasks run in parallel in the load_group
3. **Truncate Before Insert**: Ensures idempotency
4. **Materialized Views**: Pre-computed analytics for fast queries

---

## Future Enhancements

1. Add data quality checks after transformations
2. Implement incremental loading instead of full refresh
3. Add email alerts on failures
4. Create additional analytics views
5. Implement data lineage tracking
6. Add retry logic with exponential backoff

---

## Connection Setup

### Airflow Connections Required

1. **aws_conn_id** (Amazon Web Services)
   - Connection Type: AWS
   - AWS Access Key ID: (your key)
   - AWS Secret Access Key: (your secret)

2. **my_snowflake_conn** (Snowflake)
   - Connection Type: Snowflake
   - Account: UNTVDPP-YF79857
   - User: CRAZYTORI
   - Password: (your password)
   - Database: AIRFLOW_EXAM_DEC
   - Schema: PUBLIC
   - Role: data_engineer
   - Warehouse: EXAM_WH

---

## Running the Pipeline

1. **Trigger the DAG manually** or wait for scheduled run
2. **Monitor progress** in Airflow UI
3. **Verify data** in Snowflake:
   ```sql
   SELECT COUNT(*) FROM AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_SALES;
   SELECT COUNT(*) FROM AIRFLOW_EXAM_DEC.CLEANSED_LAYER.CLEANED_PRODUCTS;
   SELECT COUNT(*) FROM AIRFLOW_EXAM_DEC.ANALYTICS_LAYER.PRODUCT_SALES;
   ```

---

## Summary

This ETL pipeline provides:
- ✅ Automated data extraction from S3
- ✅ Comprehensive data cleaning and validation
- ✅ Business logic implementation (discount calculations, aggregations)
- ✅ Reliable loading to Snowflake data warehouse
- ✅ Organized task groups for maintainability
- ✅ Proper error handling and logging
- ✅ Star schema implementation
- ✅ Pre-built analytics views for reporting

**Total Pipeline Execution Time**: ~30-40 seconds
**Data Volume**: ~2,500 rows processed
**Success Rate**: 100% (after fixes implemented)
