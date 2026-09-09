import argparse
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
# from time import perf_counter
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

# INFO 레벨 로그 안 보기
spark.sparkContext.setLogLevel("WARN")


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

# 7일차 관찰 시작
# 전체 orders는 유지하고 최초 12건만 파티션 관찰에 사용합니다.
# 1~4교시 관찰 코드는 복습용으로 보존하고, 현재는 5교시를 실행합니다.
# print("\n[1교시] 입력 파티션과 파티션별 주문 수")
sample_orders = orders.filter(F.col("order_id") <= 12)
# print("sample_rows =", sample_orders.count())
# sample_orders.select("order_id", "product_id", "amount").orderBy("order_id").show()
# print("input_partitions =", sample_orders.rdd.getNumPartitions())

# partition_rows = sample_orders.select(
#     "order_id",
#     "product_id",
#     "quantity",
#     "amount",
#     F.spark_partition_id().alias("partition_id"),
# )
# partition_rows.show()
# partition_sizes = partition_rows.groupBy("partition_id").agg(
#     F.count("*").alias("row_count")
# )
# partition_sizes.orderBy("partition_id").show()

# print("\n[2교시] repartition(4)와 coalesce(2) 비교")
# split_orders = sample_orders.repartition(4)
# print("split_partitions =", split_orders.rdd.getNumPartitions())

# split_rows = split_orders.select(
#     "order_id", "product_id", F.spark_partition_id().alias("partition_id")
# )
# split_rows.show()
# split_rows.groupBy("partition_id").agg(
#     F.count("*").alias("row_count")
# ).orderBy("partition_id").show()

# merged_orders = split_orders.coalesce(2)
# print("merged_partitions =", merged_orders.rdd.getNumPartitions())
# print("split_rows =", split_orders.count())
# print("merged_rows =", merged_orders.count())
# merged_orders.select(
#     "order_id", "product_id", F.spark_partition_id().alias("partition_id")
# ).show()

# split_sales = split_orders.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue")
# )
# merged_sales = merged_orders.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue")
# )
# split_sales.orderBy("product_id").show()
# merged_sales.orderBy("product_id").show()

# print("split plan")
# split_orders.explain("formatted")
# print("merged plan")
# merged_orders.explain("formatted")

# 3교시는 별도 benchmark_sales.py에서 같은 전체 입력으로 측정합니다.
# print("\n[4교시] 상품별 부분 집계, Shuffle, 최종 집계")
# sample_summary = sample_orders.groupBy("product_id").agg(
#     F.count("*").alias("order_count"),
#     F.sum("amount").alias("revenue"),
# )
# sample_summary.explain("formatted")
# sample_summary.orderBy("product_id").show()

# keyed_orders = sample_orders.repartition(4, "product_id")
# print("keyed_partitions =", keyed_orders.rdd.getNumPartitions())

# keyed_rows = keyed_orders.select(
#     "order_id", "product_id", F.spark_partition_id().alias("partition_id")
# )
# keyed_rows.show()
# keyed_sizes = keyed_rows.groupBy("partition_id", "product_id").agg(
#     F.count("*").alias("row_count")
# )
# keyed_sizes.orderBy("partition_id", "product_id").show()

# keyed_orders.groupBy("product_id").agg(
#     F.count("*").alias("order_count"),
#     F.sum("amount").alias("revenue"),
# ).orderBy("product_id").show()

# print("\n[5교시 1단계] 연결할 주문과 상품 기준표")
# print("=" * 30, "조인 대신 테이블 상태 점검.", "=" *30,)
# sample_orders.select("order_id", "product_id", "quantity", "amount").orderBy("order_id").show()
# products.select("product_id", "name", "category").orderBy("product_id").show()

# print("\n[5교시 2단계] product_id로 inner Join")
# sample_enriched = sample_orders.join(products, on="product_id", how="inner")
# sample_enriched.printSchema()
# print("joined_rows =", sample_enriched.count())

# print("\n[5교시 3단계] 주문에 상품명과 분류 표시")
# print("=" * 30, "Product_id 를 사용해서 전체 컬럼 조회.", "=" * 30,)
# sample_named_rows = sample_enriched.select(
#     "order_id", "product_id", "name", "category", "amount"
# ).withColumnRenamed("name", "product_name")
# sample_named_rows.orderBy("order_id").show()

# print("\n[5교시 4단계] 최초 12건의 상품별 주문 횟수와 매출")
# sample_by_product = sample_enriched.groupBy("product_id", "name").agg(
#     F.count("*").alias("order_count"),
#     F.sum("amount").alias("revenue"),
# ).withColumnRenamed("name", "product_name")
# sample_by_product.orderBy("product_id").show()


print("\n[5교시 5~6단계] 전체 누적 주문의 상품별 매출과 평균")
enriched = orders.join(products, on="product_id", how="inner")
by_product = enriched.groupBy("product_id", "name","category").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
    F.sum("quantity").alias("sold_quantity"),
).withColumnRenamed("name", "product_name").withColumn(
    "average_order_amount", F.col("revenue") / F.col("order_count")
)
by_product.orderBy("product_id").show()

# print("\n[5교시 직접 해보기] 식품의 상품별 집계만 표시")
# by_product.filter(F.col("category") == "식품").orderBy("product_id").show()

# print("\n[6교시 1단계 힌트 없는 일반 조인]")
# normal_join = sample_orders.join(products, "product_id", "inner")
# print("normal join plan")
# normal_join.explain("formatted")

# print("\n[6교시 2단계 힌트 있는 브로드케스트 조인]")
# broadcast_join = sample_orders.join(F.broadcast(products), "product_id", "inner")
# print("broadcast join plan")
# broadcast_join.explain("formatted")

# print("\n[6교시 4단계 두 조인의 결과 비교]")
# normal_join.select("order_id", "product_id", "name", "amount").orderBy("order_id").show()
# broadcast_join.select("order_id", "product_id", "name", "amount").orderBy("order_id").show()

# print("\n[6교시 5단계 상품별 매출 두개를 직접 집계 후 확인]")
# normal_sales = normal_join.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue")
# )
# broadcast_sales = broadcast_join.groupBy("product_id").agg(
#     F.sum("amount").alias("revenue")
# )
# normal_sales.orderBy("product_id").show()
# broadcast_sales.orderBy("product_id").show()

# enriched = orders.join(F.broadcast(products), on="product_id", how="inner")
# enriched.select("order_id", "product_id", "category", "amount").orderBy("order_id").show()


print("\n[7교시 2~3단계 분류별 집계를 만들고 작은 딕셔너리 변화 관찰]")
by_category = enriched.groupBy("category").agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("revenue"),
)
by_category.orderBy("category").show()
category_rows = by_category.orderBy("category").collect()
# print([row.asDict() for row in category_rows])

# 7일차 관찰 끝

# orders에서 특정 컬럼만 조회하기.
# orders.select("order_id", "product_id", "quantity", "amount", "order_date").show()
# where절처럼 필터링 한 후, .select로 조회하기
# orders.filter(F.col("product_id") == "B").select("quantity", "amount", "order_date").show()
#products는 조건 없이 그냥 다 조회하여 보여주기
# products.show()

# started = perf_counter()
# measured_orders = (
#     spark.read.schema(order_schema).option("header", True)
#     .csv((data_dir / "raw" / "orders.csv").as_uri())
#     .withColumn("amount", F.col("quantity") * F.col("unit_price"))
# )

# 주문 횟수는 주문 행 수이며, 판매 수량과 구분합니다.
# product_totals = orders.groupBy("product_id").agg(
#     F.count("*").alias("order_count"),
#     F.sum("amount").alias("revenue"),
#     F.sum("quantity").alias("sold_quantity"),
# )
# 상품 목록을 연결해 새로 등록한 메뉴도 표시합니다.
# full 조인은 상품 정보가 빠진 입력에서도 기존 주문 집계를 보존합니다.

# by_product = (
#     products.join(product_totals, on="product_id", how="full")
#     .fillna(0, subset=["order_count", "revenue", "sold_quantity"])
#     .withColumn(
#         "average_order_amount",
#         F.when(F.col("order_count") > 0, F.col("revenue") / F.col("order_count")),
#     )
# )
# by_product.explain()


# 기존 화면의 name·category 필드와 주문 없는 C·D 메뉴도 유지합니다.
# 5교시의 by_product 계산 결과에 상품 목록을 연결하는 표시용 DataFrame입니다.
dashboard_products = (
    products.join(by_product.drop("category"), on="product_id", how="full")
    .withColumn("product_name", F.coalesce(F.col("product_name"), F.col("name")))
    .fillna(0, subset=["order_count", "revenue", "sold_quantity"])
)

# 5교시 7단계: JSON의 기존 표도 같은 입력 기준으로 저장하는 데 필요한 계산입니다.
# 전날의 중간 출력은 주석 상태로 유지합니다.
# print("로그기록을 확인해주세요!")

# 시간대별 주문 횟수와 매출
hourly_sales = (
    orders.withColumn("order_hour", F.hour("ordered_at"))
    .groupBy("order_hour")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").alias("revenue"),
    )
)

# 전체 매출을 한 행으로 계산
total_sales = orders.agg(
    F.sum("amount").alias("total_revenue")
)

# 각 시간대에 전체 매출을 붙이고 비중 계산
by_hour = (
    hourly_sales.crossJoin(total_sales)
    .withColumn(
        "revenue_share_pct",
        F.when(
            F.col("total_revenue") > 0,
            F.round(
                F.col("revenue") / F.col("total_revenue") * 100,
                2,
            ),
        ).otherwise(0.0),
    )
    .drop("total_revenue")
)

# 터미널에서 확인
by_hour.orderBy("order_hour").show()


logs = spark.read.text((data_dir / "raw" / "access.log").as_uri())
# logs.show(truncate=False)
log_pattern = r"^(\S+) (\S+) (\S+) (\d+) (\d+)$"

# extracted = logs.select(
#     F.regexp_extract("value", log_pattern, 1).alias("requested_at_text"),
#     F.regexp_extract("value", log_pattern, 2).alias("method"),
#     F.regexp_extract("value", log_pattern, 3).alias("path"),
# )
# extracted.show(truncate=False)

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

# parsed.select("visit_date", "path", "status", "duration_ms").show()
# page_views.orderBy("visit_date").show()
# by_day.orderBy("order_date").show()

# # 세 집계를 모두 만든 다음 미리보기와 JSON 저장 데이터를 준비합니다.
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
    "by_product": [row.asDict() for row in dashboard_products.orderBy("product_id").collect()],
    "by_day": [row.asDict() for row in by_day.orderBy("order_date").collect()],
    "page_views": [row.asDict() for row in page_views.orderBy("visit_date").collect()],
    "by_category": [row.asDict() for row in by_category.orderBy("category").collect()],
    "by_hour": [row.asDict() for row in by_hour.orderBy("order_hour").collect()],
}
# # summary["generated_at"] = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")

output_dir = data_dir / "marts"
output_dir.mkdir(parents=True, exist_ok=True)
with (output_dir / "dashboard.json").open("w", encoding="utf-8") as stream:
    json.dump(summary, stream, ensure_ascii=False, indent=2)

print("\n[5교시 7단계] 상품명이 포함된 JSON 저장 완료")
print("all_rows =", summary["order_count"])
print("dashboard_json =", output_dir / "dashboard.json")

# 모든 집계와 JSON 저장을 마친 뒤 마지막에 Spark를 종료합니다.
spark.stop()
