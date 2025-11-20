#1 pip install sqlalchemy psycopg2-binary pandas boto3
import pandas as pd
import psycopg2


df_data_raw = pd.read_csv('data/sales_data.csv') # -----> E
#print(df_data_raw.head())  first 5 rows
#print(df_data_raw.columns)
# prints as: Index(['Order ID', 'Customer Id', 'Order Date', 'amount', 'quantity',
#        'product id', 'DisKount', 'profit', 'shipping Days'],
#       dtype='object')

#2.standartization/ normalization

df_data_raw.columns = df_data_raw.columns.str.lower().str.replace(' ', '_')
df_data_raw = df_data_raw.rename(columns={"amount": "sales_amount","DisKount" : "discount"}) #------T
df_data_raw["sales_amount"] = df_data_raw["sales_amount"].fillna(0)
df_data_raw["total_revenue"] = df_data_raw["sales_amount"]*df_data_raw["quantity"]

cleaned_data = df_data_raw[df_data_raw["total_revenue"] > 0]
print(cleaned_data.head(10))
print(cleaned_data.columns)


#3.load in data
#-------L
connection = psycopg2.connect(
    host="localhost",
    database="sales_db",
    user="postgres",
    password=".....",
)

cursor = connection.cursor() # we sent SQL through it to the db
cursor.execute(""" CREATE TABLE IF NOT EXISTS sales_summary(product_id INT, quantity INT, total_revenue NUMERIC) """)

for _, row in cleaned_data.iterrows():
    cursor.execute("""INSERT INTO sales_summary(product_id,quantity,total_revenue) VALUES (%s, %s, %s) """,
                            (row["product_id"], row["quantity"], row["total_revenue"]))

connection.commit()
cursor.close()
connection.close()

