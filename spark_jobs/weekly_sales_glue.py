import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, explode, sum as _sum, count as _count,
    to_timestamp, date_format
)
import boto3
from decimal import Decimal

# =========================
# GLUE SETUP
# =========================
args        = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# =========================
# CONFIG
# =========================
REGION       = "us-east-1"
BUCKET       = "easycart1-proj-nci"
ORDERS_TABLE = "Orders"
OUTPUT_KEY   = "analytics/weekly_sales_output.json"

# =========================
# READ FROM DYNAMODB
# =========================
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table    = dynamodb.Table(ORDERS_TABLE)

response = table.scan()
orders   = response.get("Items", [])

while "LastEvaluatedKey" in response:
    response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
    orders.extend(response.get("Items", []))

def decimal_to_float(obj):
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

orders = [decimal_to_float(o) for o in orders]

# =========================
# HANDLE EMPTY
# =========================
if not orders:
    result = {
        "total_orders":     0,
        "total_revenue":    0.0,
        "total_items_sold": 0,
        "top_category":     "N/A",
        "daily_sales":      [],
        "category_sales":   [],
        "top_products":     []
    }
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=json.dumps(result, indent=4), ContentType="application/json")
    print("No orders found. Empty result written to S3.")
    job.commit()
    sys.exit(0)

# =========================
# SPARK DATAFRAME
# =========================
df = spark.createDataFrame(orders)
df = df.withColumn("created_ts", to_timestamp(col("created_at")))

# =========================
# TOTAL METRICS
# =========================
total_orders  = df.count()
total_revenue = df.agg(_sum("total_amount")).collect()[0][0] or 0.0

# =========================
# EXPLODE ITEMS
# =========================
items_df = df.select(
    col("order_id"),
    col("created_ts"),
    explode(col("items")).alias("item")
).select(
    col("order_id"),
    col("created_ts"),
    col("item.name").alias("name"),
    col("item.price").cast("double").alias("price"),
    col("item.qty").cast("int").alias("qty"),
    col("item.category").alias("category")
).fillna({"category": "General", "name": "Unknown", "price": 0.0, "qty": 1})

items_df.cache()
total_items_sold = items_df.agg(_sum("qty")).collect()[0][0] or 0

# =========================
# DAILY SALES
# =========================
daily_sales = [
    {
        "day":     row["day"],
        "orders":  int(row["orders"]),
        "revenue": round(float(row["revenue"] or 0), 2)
    }
    for row in df.withColumn("day", date_format(col("created_ts"), "EEEE"))
                 .groupBy("day")
                 .agg(_count("order_id").alias("orders"), _sum("total_amount").alias("revenue"))
                 .collect()
]

# =========================
# CATEGORY SALES
# =========================
category_sales = [
    {
        "category":   row["category"],
        "items_sold": int(row["items_sold"] or 0),
        "revenue":    round(float(row["revenue"] or 0), 2)
    }
    for row in items_df.groupBy("category")
                       .agg(_sum("qty").alias("items_sold"), _sum(col("price") * col("qty")).alias("revenue"))
                       .collect()
]

top_category = max(category_sales, key=lambda x: x["revenue"])["category"] if category_sales else "N/A"

# =========================
# TOP PRODUCTS
# =========================
top_products = [
    {
        "name":     row["name"],
        "qty_sold": int(row["qty_sold"] or 0),
        "revenue":  round(float(row["revenue"] or 0), 2)
    }
    for row in items_df.groupBy("name")
                       .agg(_sum("qty").alias("qty_sold"), _sum(col("price") * col("qty")).alias("revenue"))
                       .orderBy(col("qty_sold").desc())
                       .limit(5)
                       .collect()
]

items_df.unpersist()

# =========================
# WRITE TO S3
# =========================
result = {
    "total_orders":     total_orders,
    "total_revenue":    round(float(total_revenue), 2),
    "total_items_sold": int(total_items_sold),
    "top_category":     top_category,
    "daily_sales":      daily_sales,
    "category_sales":   category_sales,
    "top_products":     top_products
}

s3 = boto3.client("s3", region_name=REGION)
s3.put_object(
    Bucket=BUCKET,
    Key=OUTPUT_KEY,
    Body=json.dumps(result, indent=4),
    ContentType="application/json"
)

print(f"✅ Analytics written to s3://{BUCKET}/{OUTPUT_KEY}")
job.commit()