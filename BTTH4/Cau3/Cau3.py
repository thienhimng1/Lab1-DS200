import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("E-commerce Data Analysis") \
    .getOrCreate()

# Đọc dữ liệu từ các file csv và tự suy luận kiểu dữ liệu
customers_df = spark.read.csv("Customer_List.csv", header=True, inferSchema=True, sep=";")
order_items_df = spark.read.csv("Order_Items.csv", header=True, inferSchema=True, sep=";")
order_reviews_df = spark.read.csv("Order_Reviews.csv", header=True, inferSchema=True, sep=";")
orders_df = spark.read.csv("Orders.csv", header=True, inferSchema=True, sep=";")
products_df = spark.read.csv("Products.csv", header=True, inferSchema=True, sep=";")

# 3. Phân tích số lượng đơn hàng theo quốc gia của khách hàng
orders_by_country = orders_df.join(customers_df, "Customer_Trx_ID", "inner") \
    .groupBy("Customer_Country") \
    .agg(countDistinct("Order_ID").alias("Total_Orders")) \
    .orderBy(col("Total_Orders").desc())

orders_by_country.show()