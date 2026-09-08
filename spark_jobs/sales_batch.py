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
orders.select("order_id", "product_id", "quantity", "amount", "order_date").show()
# where절처럼 필터링 한 후, .select로 조회하기
orders.filter(F.col("product_id") == "B").select("quantity", "amount", "order_date").show()
#products는 조건 없이 그냥 다 조회하여 보여주기
products.show()

# started = perf_counter()
# measured_orders = (
#     spark.read.schema(order_schema).option("header", True)
#     .csv((data_dir / "raw" / "orders.csv").as_uri())
#     .withColumn("amount", F.col("quantity") * F.col("unit_price"))
# )

# 주문 횟수는 주문 행 수이며, 판매 수량과 구분합니다.
product_totals = orders.groupBy("product_id").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
    F.sum("quantity").alias("sold_quantity"),
)
# 상품 목록을 연결해 새로 등록한 메뉴도 표시합니다.
# full 조인은 상품 정보가 빠진 입력에서도 기존 주문 집계를 보존합니다.
by_product = (
    products.join(product_totals, on="product_id", how="full")
    .fillna(0, subset=["order_count", "revenue", "sold_quantity"])
    .withColumn(
        "average_order_amount",
        F.when(F.col("order_count") > 0, F.col("revenue") / F.col("order_count")),
    )
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

parsed = logs.select(
    F.to_timestamp(F.regexp_extract("value", log_pattern, 1)).alias("requested_at"),
    F.regexp_extract("value", log_pattern, 2).alias("method"),
    F.regexp_extract("value", log_pattern, 3).alias("path"),
    F.regexp_extract("value", log_pattern, 4).cast("int").alias("status"),
    F.regexp_extract("value", log_pattern, 5).cast("long").alias("duration_ms"),

).withColumn("visit_date", F.date_format("requested_at", "yyyy-MM-dd"))

page_views = parsed.groupBy("visit_date").agg(
    F.count("*").alias("page_views")
)

by_day = orders.groupBy("order_date").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
).withColumn("order_date", F.col("order_date").cast("string"))

parsed.select("visit_date", "path", "status", "duration_ms").show()
page_views.orderBy("visit_date").show()
by_day.orderBy("order_date").show()

# 세 집계를 모두 만든 다음 미리보기와 JSON 저장 데이터를 준비합니다.
preview = [
    row.asDict()
    for row in orders.select("order_id", "product_id", "quantity", "amount")
    .orderBy("order_id")
    .limit(10)
    .collect()
]


# measured_summary = measured_orders.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue"),
# )
# result = measured_summary.collect()
# print("읽기·집계·결과 수신 초:", perf_counter() - started)
# print(result)

# summary는 위에서 만든 집계와 preview를 사용하므로 이 위치에 둡니다.
summary = {
    "generated_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
    "order_count": orders.count(),
    "preview": preview,
    "by_product": [row.asDict() for row in by_product.orderBy("product_id").collect()],
    "by_day": [row.asDict() for row in by_day.orderBy("order_date").collect()],
    "page_views": [row.asDict() for row in page_views.orderBy("visit_date").collect()],
    "by_category": [],
}
summary["generated_at"] = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")

output_dir = data_dir / "marts"
output_dir.mkdir(parents=True, exist_ok=True)
with (output_dir / "dashboard.json").open("w", encoding="utf-8") as stream:
    json.dump(summary, stream, ensure_ascii=False, indent=2)

# 모든 집계와 JSON 저장을 마친 뒤 마지막에 Spark를 종료합니다.
spark.stop()
