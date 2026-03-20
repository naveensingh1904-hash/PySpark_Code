
# 1 Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# -----------------------------------------
#  2 Load the files (adjust the file paths)
# -----------------------------------------

# Example: If your files are uploaded as CSVs

customer_df = spark.read.format("csv").options(**{"inferSchema": True,
                                        "header": True}).load("/datasets/customer_purchases.csv")


#3 Register as SQL views
customer_df.createOrReplaceTempView("customer_purchases")

# 4. Filter customers with purchase_amount > 100
# -----------------------------------------
#df_filtered = customer_df.filter(customer_df.purchase_amount > 100)

df_filtered = spark.sql("""
    SELECT 
    customer_id,
    Sum(purchase_amount) as total_purchase
    FROM customer_purchases
    group by customer_id order by customer_id
""")

# -----------------------------------------
#5 Display the DataFrames
# -----------------------------------------

display(df_filtered)

