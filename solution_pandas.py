import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("EvolveMarketingStrategyForXYZ") \
                    .getOrCreate()

# below commented scrips can be used to read the data using pandas
'''
# Read 'customer.csv' into a Pandas DataFrame
pd_df_customers = pd.read_csv("dbfs:/FileStore/east_vantage/customer.csv")
# Read 'sales.csv' into a Pandas DataFrame
pd_df_sales = pd.read_csv("dbfs:/FileStore/east_vantage/sales.csv")
# Read 'items.csv' into a Pandas DataFrame
pd_df_items = pd.read_csv("dbfs:/FileStore/east_vantage/items.csv")
# Read 'orders.csv' into a Pandas DataFrame
pd_df_orders = pd.read_csv("dbfs:/FileStore/east_vantage/orders.csv")
'''

# Read the 'customer.csv' file into a DataFrame
df_customers = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/east_vantage/customer.csv")
# Read the 'sales.csv' file into a DataFrame
df_sales = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/east_vantage/sales.csv")
# Read the 'items.csv' file into a DataFrame
df_items = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/east_vantage/items.csv")
# Read the 'orders.csv' file into a DataFrame
df_orders = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/east_vantage/orders.csv")


# below commented codes are the script to read data from SQLite database
'''
import sqlite3

# Define the SQLite database file path
database_path = 'path_to_database'

# Create a connection to the SQLite database
conn = sqlite3.connect(database_path)

# Read customers data from the SQLite database into a Pandas DataFrame
query_customers = "SELECT * FROM customers"
df_customers = pd.read_sql_query(query_customers, conn)

# Read sales data from the SQLite database into a Pandas DataFrame
query_sales = "SELECT * FROM sales"
df_sales = pd.read_sql_query(query_sales, conn)

# Read items data from the SQLite database into a Pandas DataFrame
query_items = "SELECT * FROM items"
df_items = pd.read_sql_query(query_items, conn)

# Read orders data from the SQLite database into a Pandas DataFrame
query_orders = "SELECT * FROM orders"
df_orders = pd.read_sql_query(query_orders, conn)
'''

# change to pandas dataframe
pd_df_customers = df_customers.toPandas()
pd_df_sales = df_sales.toPandas()
pd_df_items = df_items.toPandas()
pd_df_orders = df_orders.toPandas()

# Group and aggregate data
pd_df_grouped_data = pd_df_orders.groupby(["item_id", "sales_id"])["quantity"].count().reset_index()

# no value with decimal point
pd_df_grouped_data["quantity"] = pd_df_grouped_data["quantity"].round(0)

# change datatype of age to int
pd_df_customers['age'] = pd_df_customers['age'].astype(int)

# filter the age group
pd_df_customers = pd_df_customers[(pd_df_customers['age'] >= 18) & (pd_df_customers['age'] <= 35)]

# Join DataFrames
pd_df_customers_sales = pd_df_customers.merge(pd_df_sales, on="customer_id", how="inner")[["customer_id", "age", "sales_id"]]

# Join DataFrames and drop unnecessary columns
pd_df_intermediate = pd_df_grouped_data.merge(pd_df_customers_sales, on="sales_id", how="left")
pd_df_intermediate = pd_df_intermediate.drop("sales_id", axis=1)

# Join with df_items and select desired columns
pd_df_final = pd_df_intermediate.merge(pd_df_items, on="item_id", how="left")
# take all records with quantities > 0
pd_df_final = pd_df_final[(pd_df_final['quantity'] > 0)]
pd_df_final = pd_df_final[["customer_id", "age", "item_name", "quantity"]].rename(columns={"item_name": "item"})
pd_df_final = pd_df_final.sort_values(by="customer_id")

# Display the final DataFrame
print(pd_df_final)

# use below commented script to save the data using pandas
'''
pd_df_final.to_csv("dbfs:/FileStore/east_vantage/output_pandas.csv)
'''
# converting to dataframe to save the data
df_final_spark = spark.createDataFrame(pd_df_final)

# write the data to the output location
df_final_spark.write.mode("overwrite").csv("dbfs:/FileStore/east_vantage/output_pandas")
