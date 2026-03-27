import boto3
import json
from decimal import Decimal
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, sum as spark_sum, count, lit
)

# =========================
# CONFIG
# =========================
REGION = "us-east-1"   # change if needed
ORDERS_TABLE = "Orders"
ANALYTICS_TABLE = "SalesAnalyticsWeekly"


# =========================
# HELPERS
# =========================
def decimal_to_float(obj):
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def normalize_order_items(items):
    """
    Handles both:
    1. normal python dict items
    2. DynamoDB-typed item structure (M/S/N)
    """
    normalized = []

    for item in items:
        if "M" in item:
            data = item["M"]

            normalized.append({
                "name": data.get("name", {}).get("S", "Unknown"),
                "category": data.get("category", {}).get("S", "General"),
                "price": float(data.get("price", {}).get("N", 0)),
                "qty": int(data.get("qty", {}).get("N", 1)),
            })
        else:
            normalized.append({
                "name": item.get("name", "Unknown"),
                "category": item.get("category", "General"),
                "price": float(item.get("price", 0)),
                "qty": int(item.get("qty", 1)),
            })

    return normalized


# =========================
# LOAD ORDERS FROM DYNAMODB
# =========================
def load_orders():
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(ORDERS_TABLE)

    response = table.scan()
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    cleaned_orders = []
    now = datetime.utcnow()
    last_7_days = now - timedelta(days=7)

    for order in items:
        order = decimal_to_float(order)

        created_at = order.get("created_at")
        if not created_at:
            continue

        try:
            order_date = datetime.fromisoformat(created_at)
        except:
            continue

        if order_date < last_7_days:
            continue

        total_amount = float(order.get("total_amount", 0))
        day = order_date.strftime("%A")

        normalized_items = normalize_order_items(order.get("items", []))

        cleaned_orders.append({
            "order_id": order.get("order_id"),
            "created_at": created_at,
            "day": day,
            "total_amount": total_amount,
            "items": normalized_items
        })

    return cleaned_orders


# =========================
# MAIN SPARK JOB
# =========================
def run_spark_job():
    spark = SparkSession.builder \
        .appName("WeeklySalesAnalytics") \
        .getOrCreate()

    orders = load_orders()

    if not orders:
        print("No recent orders found.")
        return

    # -------------------------
    # Orders DF
    # -------------------------
    orders_df = spark.createDataFrame(orders)

    total_orders = orders_df.count()
    total_revenue = orders_df.agg(spark_sum("total_amount")).collect()[0][0]

    daily_sales_df = orders_df.groupBy("day").agg(
        count("*").alias("orders"),
        spark_sum("total_amount").alias("revenue")
    )

    daily_sales = [
        {
            "day": row["day"],
            "orders": int(row["orders"]),
            "revenue": float(row["revenue"])
        }
        for row in daily_sales_df.collect()
    ]

    # -------------------------
    # Flatten items
    # -------------------------
    items_df = orders_df.select(explode("items").alias("item")).select(
        col("item.name").alias("name"),
        col("item.category").alias("category"),
        col("item.price").alias("price"),
        col("item.qty").alias("qty")
    )

    total_items_sold = items_df.agg(spark_sum("qty")).collect()[0][0]

    # category analytics
    category_df = items_df.withColumn("revenue", col("price") * col("qty")) \
        .groupBy("category") \
        .agg(
            spark_sum("qty").alias("items_sold"),
            spark_sum("revenue").alias("revenue")
        ) \
        .orderBy(col("revenue").desc())

    category_sales = [
        {
            "category": row["category"],
            "items_sold": int(row["items_sold"]),
            "revenue": float(row["revenue"])
        }
        for row in category_df.collect()
    ]

    top_category = category_sales[0]["category"] if category_sales else "N/A"

    # product analytics
    product_df = items_df.withColumn("revenue", col("price") * col("qty")) \
        .groupBy("name") \
        .agg(
            spark_sum("qty").alias("qty_sold"),
            spark_sum("revenue").alias("revenue")
        ) \
        .orderBy(col("qty_sold").desc())

    top_products = [
        {
            "name": row["name"],
            "qty_sold": int(row["qty_sold"]),
            "revenue": float(row["revenue"])
        }
        for row in product_df.limit(5).collect()
    ]

    # -------------------------
    # Save analytics result
    # -------------------------
    analytics_result = {
        "report_id": "weekly_latest",
        "generated_at": datetime.utcnow().isoformat(),
        "total_orders": int(total_orders),
        "total_revenue": float(total_revenue or 0),
        "total_items_sold": int(total_items_sold or 0),
        "top_category": top_category,
        "daily_sales": daily_sales,
        "category_sales": category_sales,
        "top_products": top_products
    }

    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    analytics_table = dynamodb.Table(ANALYTICS_TABLE)
    analytics_table.put_item(Item=analytics_result)

    print("Weekly analytics saved successfully.")
    print(json.dumps(analytics_result, indent=2))

    spark.stop()


if __name__ == "__main__":
    run_spark_job()