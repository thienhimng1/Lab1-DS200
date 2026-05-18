import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    expr,
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

# Làm sạch dữ liệu Review_Score
clean_reviews_df = order_reviews_df.withColumn(
    "Review_Score_int",
    expr("try_cast(Review_Score as int)")
)

# Loại bỏ dữ liệu không hợp lệ
clean_reviews_df = clean_reviews_df.filter(
    (col("Review_Score_int").isNotNull()) &
    (col("Review_Score_int").between(1, 5))
)

# 5. Thống kê điểm đánh giá trung bình, số lượng đánh giá theo từng mức (ví dụ: 1 đến 5).
avg_score = clean_reviews_df.agg(
    round(avg("Review_Score_int"), 2).alias("Average_Review_Score")
)

print("\n===== AVERAGE REVIEW SCORE =====")
avg_score.show()

score_distribution = clean_reviews_df \
    .groupBy("Review_Score_int") \
    .agg(count("*").alias("Total_Reviews")) \
    .orderBy("Review_Score_int")

print("\n===== REVIEW SCORE DISTRIBUTION =====")
score_distribution.show()