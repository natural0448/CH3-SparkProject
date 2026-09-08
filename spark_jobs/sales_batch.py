import argparse
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from time import perf_counter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", required=True)
args = parser.parse_args()
data_dir = Path(args.data_dir).resolve()

spark = (
    SparkSession.builder.appName("order-insight-sales")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .getOrCreate()
)

order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", LongType()),
    StructField("ordered_at", StringType()),
])

# product schema는 단순하므로 문자로 나열하고 끝낼수 있다.
product_schema = "product_id string, name string, category string"

raw_orders = spark.read.schema(order_schema).option("header", True).csv(
    (data_dir / "raw" / "orders.csv").as_uri()
)
products = spark.read.schema(product_schema).json(
    (data_dir / "raw" / "products.jsonl").as_uri()
)
orders = (
    raw_orders.withColumn("amount", F.col("quantity") * F.col("unit_price"))
    .withColumn("ordered_at", F.to_timestamp("ordered_at"))
    .withColumn("order_date", F.to_date("ordered_at"))
)

# orders에서 특정 컬럼만 조회하기.
orders.select("order_id", "product_id", "quantity", "amount", "order_date").show()\
# where절처럼 필터링 한 후, .select로 조회하기
orders.filter(F.col("product_id") == "B").select("quantity", "amount", "order_date").show()
#products는 조건 없이 그냥 다 조회하여 보여주기
products.show()

preview = [
    row.asDict()
    for row in orders.select("order_id", "product_id", "quantity", "amount")
    .orderBy("order_id")
    .limit(10)
    .collect()
]

# started = perf_counter()
# measured_orders = (
#     spark.read.schema(order_schema).option("header", True)
#     .csv((data_dir / "raw" / "orders.csv").as_uri())
#     .withColumn("amount", F.col("quantity") * F.col("unit_price"))
# )

summary = {
    "generated_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
    "order_count": orders.count(),
    "preview": preview,
    "by_product": [],
    "by_day": [],
    "page_views": [],
    "by_category": [],
}

output_dir = data_dir / "marts"
output_dir.mkdir(parents=True, exist_ok=True)

with (output_dir / "dashboard.json").open("w", encoding="utf-8") as stream:
    json.dump(summary, stream, ensure_ascii=False, indent=2)


output_dir = data_dir / "marts"
output_dir.mkdir(parents=True, exist_ok=True)
with (output_dir / "dashboard.json").open("w", encoding="utf-8") as stream:
    json.dump(summary, stream, ensure_ascii=False, indent=2)

# 집계함수를 활용해서 상품별 매출총액을 구합니다.
by_product = orders.groupBy("product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
    F.sum("quantity").alias("sold_quantity"),
)
by_product.explain()
by_product.orderBy("product_id").show()

print("로그기록을 확인해주세요!")
logs = spark.read.text((data_dir / "raw" / "access.log").as_uri())
# logs.show(truncate=False)
log_pattern = r"^(\S+) (\S+) (\S+) (\d+) (\d+)$"

extracted = logs.select(
    F.regexp_extract("value", log_pattern, 1).alias("requested_at_text"),
    F.regexp_extract("value", log_pattern, 2).alias("method"),
    F.regexp_extract("value", log_pattern, 3).alias("path"),
)
extracted.show(truncate=False)


# 집계함수에 파싱칼럼을 생성합니다.(.withCalum())
by_product2 = orders.groupBy("product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
    F.sum("quantity").alias("sold_quantity"),
).withColumn(
    "average_order_amount", F.col("revenue") / F.col("order_count")
)
by_product2.explain()
by_product2.orderBy("product_id").show()

# measured_summary = measured_orders.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue"),
# )
# result = measured_summary.collect()
# print("읽기·집계·결과 수신 초:", perf_counter() - started)
# print(result)

# 커넥션 끊기
spark.stop()

