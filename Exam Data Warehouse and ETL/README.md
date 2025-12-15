# DATA WAREHOUSE AND ETL EXAM PROCESS 
 
## -> ETL Airflow

 1.astro dev init
 
 2.astro dev start
 
3.connect Airflow to AWS


## Pycharm Project

5. include -> create config.yaml
   
         aws_conn_id : aws_conn_id
        
        s3:
          bucket: course-data-warehouse-viki
          folder: Exam/
          output_folder : OutputExam
    

6. include -> create logger.py

8. include-> etl -> extract_s3
9. dags -> globo_retail_etl_dag    -> extract_data_group()

<img width="692" height="432" alt="image" src="https://github.com/user-attachments/assets/a09c9068-78c8-4fcd-8d18-a1f730d96855" />


10. include -> validations -> sales_schema_validations.py / product_schema_validation.py

for sales i did only output validations, because input rows need to be changed

input sales file:

<img width="807" height="319" alt="image" src="https://github.com/user-attachments/assets/c5fec49b-d492-4517-bcca-61a207b97e55" />

input product file is json:

<img width="500" height="701" alt="image" src="https://github.com/user-attachments/assets/7e6c9ee3-2fc5-42d3-8863-adb49a039139" />

11.include->etl->transform.py -> def transform_sales_data /transform_product_data

12.dags-> globo_retail_etl_dag -> transform_group()

Succsesful test Run of DAG till now:

<img width="729" height="281" alt="image" src="https://github.com/user-attachments/assets/ff783a99-8b6c-4162-ad5d-a5e7731ec67d" />

13.Merge the two tables:

14.And load all the files in s3:

<img width="1009" height="365" alt="image" src="https://github.com/user-attachments/assets/0c2f4a0c-5afc-436c-9993-84b0670d7a9b" />


15.The output files could be found in S3 

<img width="1553" height="569" alt="image" src="https://github.com/user-attachments/assets/78b7936b-77c8-43ea-b54b-1f5006942463" />



16.The transformed products :

<img width="589" height="206" alt="image" src="https://github.com/user-attachments/assets/6a89a54c-9c14-4677-ab88-d79b7e2bb011" />

17.The transformed sales:

<img width="733" height="398" alt="image" src="https://github.com/user-attachments/assets/d28d7036-4eca-49db-9d71-331c1f7443f7" />


## -> ELT - Snowflake 

1.Created a role

2.created the warehouse

3.Create the database

4.create the schemas

5.create the format

6. create aws connection

7. create tables and insert into them the cleaned tables from the previous steps

9. build star schema
    
9. create ERD in DBeaver to check the connection 


<img width="1708" height="906" alt="image" src="https://github.com/user-attachments/assets/ec5ee71c-b24f-46e5-959c-9f4956bd8b8b" />


10.Create Materialized Analytical Views
## 📊 Presentation Layer – Materialized Views

### MV_SALES_BY_REGION_MONTH
Provides monthly sales aggregation by region, including total orders and total revenue, enabling regional performance analysis over time.

### MV_TOP_PRODUCTS_BY_REVENUE
Aggregates total revenue by product and categorizes products into Top, Middle, and Low revenue tiers based on sales thresholds.

### MV_REVENUE_TREND
Summarizes total revenue by year and month to support analysis of revenue trends over time.

### MV_CATEGORY_PERFORMANCE
Displays category-level sales performance by month and year, including total quantity sold and total sales value.




