import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    sum as _sum,
    count as _count,
    to_timestamp,
    date_format
)

# =========================
# FILE PATHS
# =========================
INPUT_PATH = "analytics_data/orders.json"
OUTPUT_PATH = "analytics_data/weekly_sales_output.json"

# =========================
# START SPARK
# =========================
spark = SparkSession.builder \
    .appName("EasyCartWeeklySalesAnalytics") \
    .getOrCreate()

# =========================
# READ INPUT (LET SPARK INFER STRUCTURE)
# =========================
df = spark.read.json(INPUT_PATH)

print("DEBUG → Total orders read:", df.count())
df.show(5, truncate=False)

# =========================
# HANDLE EMPTY FILE
# =========================
if df.count() == 0:
    result = {
        "total_orders": 0,
        "total_revenue": 0,
        "total_items_sold": 0,
        "top_category": "N/A",
        "daily_sales": [],
        "category_sales": [],
        "top_products": []
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(result, f, indent=4)

    spark.stop()
    exit()

# =========================
# PARSE DATES
# =========================
df = df.withColumn("created_ts", to_timestamp(col("created_at")))

# =========================
# TOTAL METRICS
# =========================
total_orders = df.count()
total_revenue = df.agg(_sum("total_amount")).collect()[0][0] or 0

# =========================
# EXPLODE ITEMS (ROBUST)
# =========================
items_df = df.select(
    col("order_id"),
    col("created_ts"),
    explode(col("items")).alias("item")
)

# Flatten item safely
items_df = items_df.select(
    col("order_id"),
    col("created_ts"),
    col("item.name").alias("name"),
    col("item.price").cast("double").alias("price"),
    col("item.qty").cast("int").alias("qty"),
    col("item.category").alias("category")
)

# Fill missing values
items_df = items_df.fillna({
    "category": "General",
    "name": "Unknown",
    "price": 0,
    "qty": 1
})

print("DEBUG → Flattened Items")
items_df.show(20, truncate=False)

total_items_sold = items_df.agg(_sum("qty")).collect()[0][0] or 0

# =========================
# DAILY SALES
# =========================
daily_sales_df = df.withColumn("day", date_format(col("created_ts"), "EEEE")) \
    .groupBy("day") \
    .agg(
        _count("order_id").alias("orders"),
        _sum("total_amount").alias("revenue")
    )

daily_sales = [
    {
        "day": row["day"],
        "orders": int(row["orders"]),
        "revenue": round(float(row["revenue"] or 0), 2)
    }
    for row in daily_sales_df.collect()
]

# =========================
# CATEGORY SALES
# =========================
category_sales_df = items_df.groupBy("category").agg(
    _sum("qty").alias("items_sold"),
    _sum(col("price") * col("qty")).alias("revenue")
)

category_sales = [
    {
        "category": row["category"],
        "items_sold": int(row["items_sold"] or 0),
        "revenue": round(float(row["revenue"] or 0), 2)
    }
    for row in category_sales_df.collect()
]

# Top category
if category_sales:
    top_category = max(category_sales, key=lambda x: x["revenue"])["category"]
else:
    top_category = "N/A"

# =========================
# TOP PRODUCTS
# =========================
top_products_df = items_df.groupBy("name").agg(
    _sum("qty").alias("qty_sold"),
    _sum(col("price") * col("qty")).alias("revenue")
).orderBy(col("qty_sold").desc())

top_products = [
    {
        "name": row["name"],
        "qty_sold": int(row["qty_sold"] or 0),
        "revenue": round(float(row["revenue"] or 0), 2)
    }
    for row in top_products_df.limit(5).collect()
]

# =========================
# FINAL OUTPUT
# =========================
result = {
    "total_orders": total_orders,
    "total_revenue": round(float(total_revenue), 2),
    "total_items_sold": int(total_items_sold),
    "top_category": top_category,
    "daily_sales": daily_sales,
    "category_sales": category_sales,
    "top_products": top_products
}

with open(OUTPUT_PATH, "w") as f:
    json.dump(result, f, indent=4)

print("✅ Weekly sales analytics written to:", OUTPUT_PATH)

spark.stop()