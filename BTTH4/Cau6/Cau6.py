import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    to_timestamp,
    sum,
    round
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

# 6. Phân tích số lượng đơn hàng nhóm theo năm, tháng đặt hàng và quốc gia của khách hàng (Hiển thị theo năm tăng dần, tháng giảm dần, quốc gia tăng dần)
# Lọc các đơn hàng năm 2024
orders_2024 = orders_df \
    .withColumn("Year", year(to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm"))) \
    .filter(col("Year") == 2024)

# Tính doanh thu theo danh mục
revenue_2024_by_category = order_items_df \
    .join(orders_2024, "Order_ID", "inner") \
    .join(products_df, "Product_ID", "inner") \
    .withColumn("Revenue", col("Price") + col("Freight_Value")) \
    .groupBy("Product_Category_Name") \
    .agg(sum("Revenue").alias("Total_Revenue_2024")) \
    .orderBy(col("Total_Revenue_2024").desc())

revenue_2024_by_category.show()