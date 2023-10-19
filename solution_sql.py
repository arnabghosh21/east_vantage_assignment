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

# create temp views for all the dataframes
df_customers.createOrReplaceTempView("customers")
df_sales.createOrReplaceTempView("sales")
df_items.createOrReplaceTempView("items")
df_orders.createOrReplaceTempView("orders")

# sql query to get the output
df_result = spark.sql("""
                    with customer_sales as(
                        select customers.customer_id, customers.age, sales.sales_id from customers inner join sales on customers.customer_id = sales.customer_id
                        where (customers.age >= 18) and (customers.age <= 35)
                            ),
                        grouped_orders as (
                            select item_id, sales_id, round(count(quantity), 0) as quantity from orders
                            group by item_id, sales_id
                        ),
                        intermediate as (
                            select item_id, quantity, customer_id, age from grouped_orders left join customer_sales on grouped_orders.sales_id = customer_sales.sales_id
                        ),
                        final as (
                            select customer_id, age, item_name as item, quantity from intermediate left join items on intermediate.item_id = items.item_id
                            where quantity > 0
                            order by customer_id, item asc
                        )
                            select * from final
               """)

# Display the contents of the 'df_final' DataFrame
df_result.show()
# write the dataframe in csv format
df_result.write.mode("overwrite").option("delimter", ";") \
                                    .option("header", "true") \
                                        .csv("dbfs:/FileStore/east_vantage/output_sql")
