
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

