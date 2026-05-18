import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    year,
    month,
    to_timestamp
)

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

# 4. Phân tích số lượng đơn hàng nhóm theo năm, tháng đặt hàng (Hiển thị theo năm tăng dần, tháng giảm dần)
orders_by_year_month = orders_df \
    .withColumn("Year", year(to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm"))) \
    .withColumn("Month", month(to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm"))) \
    .groupBy("Year", "Month") \
    .agg(count("Order_ID").alias("Total_Orders")) \
    .orderBy(col("Year").asc(), col("Month").desc())

orders_by_year_month.show()