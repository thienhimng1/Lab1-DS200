from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, to_timestamp, year, month, avg, sum, datediff

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

# 2. Tính tổng số đơn hàng, tổng số lượng khách hàng và tổng số lượng người bán
total_orders = orders_df.select("Order_ID").distinct().count()
total_customers = customers_df.select("Customer_Trx_ID").distinct().count()
total_sellers = order_items_df.select("Seller_ID").distinct().count()

print(f"Tổng số đơn hàng: {total_orders}")
print(f"Tổng số lượng khách hàng: {total_customers}")
print(f"Tổng số lượng người bán: {total_sellers}")