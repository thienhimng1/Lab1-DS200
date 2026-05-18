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

# 1. Hãy đọc dữ liệu từ các file csv, sử dụng tự suy ra kiểu dữ liệu cho mỗi cột.
print("Customers:")
customers_df.printSchema()
print("Order Items:")
order_items_df.printSchema()
print("Order Reviews:")
order_reviews_df.printSchema()
print("Orders:")
orders_df.printSchema()
print("Products:")
products_df.printSchema()
