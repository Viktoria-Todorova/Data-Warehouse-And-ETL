
---

## ⚙️ Configuration File (`config.yaml`)

The ETL pipeline uses a YAML configuration file to define S3 paths, Snowflake targets, and connection IDs.  
**YAML requires a space after each colon**, and indentation defines structure.

### ✔ Example `config.yaml`

```yaml
aws_conn_id: aws_conn_id

s3:
  bucket: course-data-warehouse-viki
  folder: AirflowPipeline/

snowflake:
  conn_id: my_snowflake_conn
  database: SALES_DB_NOV_AIRFLOW
  targets:
    sales:
      schema: cleansing_layer
      table: sales
    customers:
      schema: cleansing_layer
      table: customers
    products:
      schema: cleansing_layer
      table: products
    monthly_sales:
      schema: presentation_layer
      table: monthly_sales_summary 
```

### ⚠ YAML Formatting

Always include a space after each colon

Use spaces, never tabs

Maintain consistent indentation

Incorrect formatting may produce errors like:

      TypeError: string indices must be integers, not 'str'
