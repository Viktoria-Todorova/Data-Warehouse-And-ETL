# DATA WAREHOUSE AND ETL EXAM PROCESS 
 
 ![15633](https://github.com/user-attachments/assets/bcb71e92-cc15-4d67-bdcd-6fa342bb7325)


 1.astro dev init
 
 2.astro dev start
 
 3. pip install -r requirements.txt
 
 4. connect Airflow to Snowflake and AWS via Admin -> Connections > Add connection
 
<img width="885" height="717" alt="image" src="https://github.com/user-attachments/assets/107e55df-d17b-45d8-a9cb-5daa52644cd8" />

##Pycharm Project

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

The output files could be found in S3 

<img width="1745" height="439" alt="image" src="https://github.com/user-attachments/assets/dee48cb4-7180-4acd-9fb4-80c2bdecca17" />


The transformed products :

<img width="589" height="206" alt="image" src="https://github.com/user-attachments/assets/6a89a54c-9c14-4677-ab88-d79b7e2bb011" />

The transformed sales:

<img width="733" height="398" alt="image" src="https://github.com/user-attachments/assets/d28d7036-4eca-49db-9d71-331c1f7443f7" />


# Snowflake 

1.Created a role
2.created the warehouse
3.Create the database
4.create the schemas
5.create the format
6. create aws connection
7. create tables and insert into them the cleaned tables from the previous steps
8. build star schema
9. create ERD in DBeaver to check the connection 


<img width="1708" height="906" alt="image" src="https://github.com/user-attachments/assets/ec5ee71c-b24f-46e5-959c-9f4956bd8b8b" />

#

<img width="1819" height="825" alt="image" src="https://github.com/user-attachments/assets/66207a66-988c-4457-93c4-0a46018afd9e" />

