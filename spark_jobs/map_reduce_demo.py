import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", required=True)
args = parser.parse_args()

rows = [
    ("A", 100), ("A", 200), ("A", 300),
    ("B", 100), ("B", 200), ("B", 300),
    ("A", 400), ("A", 500),
    ("B", 400), ("B", 500), ("B", 600), ("B", 700),
]

spark = SparkSession.builder.appName("order-insight-map-reduce").getOrCreate()
orders = spark.createDataFrame(
    spark.sparkContext.parallelize(rows, 2),
    "product_id string, amount long",
)

summary = orders.groupBy("product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
).withColumn("average_order_amount", F.col("revenue") / F.col("order_count"))\
# 해당 작업에 대한 실행 계획에 대한 내용 확인.
summary.explain()


summary.orderBy("product_id").show()
spark.stop()