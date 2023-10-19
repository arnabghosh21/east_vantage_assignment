# Import necessary PySpark libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder \
                    .appName("EvolveMarketingStrategyForXYZ") \
                    .getOrCreate()

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
# Initialize a Spark session
spark = SparkSession.builder \
                    .appName("EvolveMarketingStrategyForXYZ") \
                    .config("spark.jars", path_to_jar) \
                    .getOrCreate()

# Define the SQLite database URL
database_url = "jdbc:sqlite:path_to_database"

# Read customers data from the SQLite database
df_customers = spark.read.format("jdbc") \
                            .option("url", database_url) \
                            .option("dbtable", "customers") \
                            .load()
# Read sales data from the SQLite database
df_sales = spark.read.format("jdbc") \
                        .option("url", database_url) \
                        .option("dbtable", "sales") \
                        .load()
# Read items data from the SQLite database
df_items = spark.read.format("jdbc") \
                        .option("url", database_url) \
                        .option("dbtable", "items") \
                        .load()
# Read orders data from the SQLite database
df_orders = spark.read.format("jdbc") \
                        .option("url", database_url) \
                        .option("dbtable", "orders") \
                        .load()
'''

# Group and aggregate data in 'df_orders' DataFrame by 'item_id' and 'sales_id'
df_grouped_data = df_orders.groupBy("item_id", "sales_id") \
                            .agg(F.count("quantity").alias("quantity")) \
                                .withColumn("quantity", F.round(F.col("quantity"), 0))

# filter customers between the age of 18 & 35
df_customers = df_customers.withColumn("age", F.col("age").cast("int"))
df_customers = df_customers.filter(F.col("age") >= 18) \
                            .filter(F.col("age") <= 35)

# Perform an inner join between 'df_customers' and 'df_sales' DataFrames based on 'customer_id'
# and select 'customer_id', 'age', and 'sales_id' columns
df_customers_sales = df_customers.join(df_sales, df_customers.customer_id == df_sales.customer_id, how="inner") \
                                    .select(df_customers.customer_id, "age", "sales_id")

# Perform a left join between 'df_grouped_data' and 'df_customers_sales' DataFrames based on 'sales_id'
# and drop the 'sales_id' column
df_intermediate = df_grouped_data.join(df_customers_sales, "sales_id", "left") \
                                    .drop("sales_id")

# Perform a left join between 'df_intermediate' and 'df_items' DataFrames based on 'item_id'
# and drop the 'item_id' column. Then, select specific columns and order by 'customer_id'.
df_final = df_intermediate.join(df_items, "item_id", "left") \
                            .drop("item_id")

df_final = df_final.filter(F.col("quantity") > 0) \
                    .select("customer_id", "age", df_final["item_name"].alias("item"), "quantity") \
                        .orderBy("customer_id", "item")

# Display the contents of the 'df_final' DataFrame
df_final.show()
# write the dataframe in csv format
df_final.write.mode("overwrite").option("delimter", ";") \
                                    .option("header", "true") \
                                        .csv("dbfs:/FileStore/east_vantage/output_pyspark")
