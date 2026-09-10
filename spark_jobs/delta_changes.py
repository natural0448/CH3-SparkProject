# spark_jobs/delta_changes.py

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
)


parser = argparse.ArgumentParser()
parser.add_argument("--data-dir",required=True,)
args = parser.parse_args()
data_dir = Path(args.data_dir).resolve()

spark = (
    SparkSession.builder
    .appName("order-insight-delta-changes")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .getOrCreate()
)

# INFO 레벨 로그 안 보기
spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", LongType(), True),
    StructField("ordered_at", StringType(), True),
])

raw_orders = spark.read.schema(order_schema).option("header", True).csv(
    (data_dir / "raw" / "orders.csv").as_uri()
)

orders = (
    raw_orders.filter(F.col("order_id") <= 12)
    .withColumn("amount",F.col("quantity") * F.col("unit_price"),)
    .withColumn("ordered_at",F.to_timestamp("ordered_at"),)
    .withColumn("order_date",F.to_date("ordered_at"),)
)

demo_path = data_dir / "demo" / "delta_orders"
first_day = orders.filter(F.col("order_date") == F.lit("2026-09-07").cast("date"))

first_day.write \
    .format("delta") \
    .mode("overwrite") \
    .save(str(demo_path))

print("\n", "="*20, "9월 7일 데이터만 업로드", "="*20)
print("after overwrite:", first_day.count())
first_day.orderBy("order_id").show(truncate=False)

next_day = orders.filter(F.col("order_date") == F.lit("2026-09-08").cast("date"))
next_day.write \
    .format("delta") \
    .mode("append") \
    .save(str(demo_path))

print("\n", "="*20, "9월 8일 데이터 추가 업로드", "="*20)
print("이번에 추가한 주문:", next_day.count())
next_day.orderBy("order_id").show(truncate=False)

# 5-5~5-6단계: 주문 1번을 수정한 뒤 최신 데이터로 결과를 확인합니다.
spark.sql(
    f"""
    UPDATE delta.`{demo_path}`
    SET
        quantity = 2,
        amount = unit_price * 2
    WHERE order_id = 1
    """
)

print("\n", "="*20, "주문 1번 수량과 금액 수정", "="*20)
current_orders = spark.read.format("delta").load(str(demo_path))
print("after update:", current_orders.count())
current_orders.orderBy("order_id").show(truncate=False)

print("\n", "="*20, "수정 후 상품별 매출", "="*20)
current_orders.groupBy("product_id").agg(
    F.sum("amount").alias("revenue")
).orderBy("product_id").show()


# 6-1~6-2단계: 최근 변경부터 버전, 작업 종류, 작업 설정을 읽습니다.
print("\n", "="*20, "Delta 변경 이력 (최신 버전부터)", "="*20)
history = spark.sql(f"DESCRIBE HISTORY delta.`{demo_path}`")

history.select(
    "version",
    "operation",
    "operationParameters",
).orderBy(
    F.col("version").desc()
).show(
    truncate=False
)

print("\n", "="*20, "주문 1번의 현재 값 확인", "="*20)
current_orders.filter(F.col("order_id") == 1).select(
    "order_id", "quantity", "amount"
).show()

spark.stop()
