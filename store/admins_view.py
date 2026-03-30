from decimal import Decimal
import boto3
import uuid
from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib import messages
from .views import admin_required
from datetime import datetime, timedelta
import json
import os
import subprocess


# =========================
# ADMIN DASHBOARD
# =========================
@admin_required
def admin_dashboard(request):
    return render(request, "admin/admin_dashboard.html")


# =========================
# HELPER: ENSURE BUCKET EXISTS
# =========================
def ensure_bucket_exists():
    s3 = boto3.client("s3", region_name=settings.S3_REGION)
    bucket_name = settings.S3_BUCKET

    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        try:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": settings.S3_REGION
                }
            )
            print(f"Bucket '{bucket_name}' created successfully.")
        except Exception as e:
            print("Bucket creation failed:", e)
            return False
    return True


# =========================
# HELPER: UPLOAD FILE TO S3
# =========================
def upload_product_image_to_s3(file_obj):
    s3 = boto3.client("s3", region_name=settings.S3_REGION)
    bucket_name = settings.S3_BUCKET

    if not ensure_bucket_exists():
        return None

    ext = file_obj.name.split(".")[-1]
    unique_name = f"product-images/{uuid.uuid4()}.{ext}"

    try:
        s3.upload_fileobj(
            file_obj,
            bucket_name,
            unique_name,
            ExtraArgs={"ContentType": file_obj.content_type}
        )
        return unique_name
    except Exception as e:
        print("S3 Upload Error:", e)
        return None


# =========================
# ADD PRODUCT
# =========================
@admin_required
def admin_add_product(request):
    categories = settings.COGNITO.get("dynamodb_tables", [])

    if request.method == "POST":
        category = request.POST.get("category")
        name = request.POST.get("name")
        description = request.POST.get("description")
        price = Decimal(request.POST.get("price"))
        image_file = request.FILES.get("image_file")

        if category not in categories:
            messages.error(request, "Invalid category selected.")
            return redirect("admin_add_product")

        if not image_file:
            messages.error(request, "Please upload an image.")
            return redirect("admin_add_product")

        s3_key = upload_product_image_to_s3(image_file)
        if not s3_key:
            messages.error(request, "Failed to upload image to S3.")
            return redirect("admin_add_product")

        dynamodb = boto3.resource("dynamodb", region_name=settings.S3_REGION)
        table = dynamodb.Table(category)

        pid = str(uuid.uuid4())

        table.put_item(
            Item={
                "product_id": pid,
                "name": name,
                "description": description,
                "price": price,
                "image": s3_key
            }
        )

        messages.success(request, "Product added successfully!")
        return redirect("admin_dashboard")

    return render(request, "admin/add_product.html", {
        "categories": categories
    })


# =========================
# VIEW / LIST ALL PRODUCTS
# =========================
@admin_required
def admin_manage_products(request):
    dynamodb = boto3.resource("dynamodb", region_name=settings.S3_REGION)
    categories = settings.COGNITO.get("dynamodb_tables", [])

    products = []

    for cat in categories:
        table = dynamodb.Table(cat)
        res = table.scan().get("Items", [])

        for item in res:
            item["category"] = cat
            products.append(item)

    return render(request, "admin/manage_products.html", {
        "products": products,
        "categories": categories
    })


# =========================
# DELETE PRODUCT
# =========================
@admin_required
def admin_delete_product(request, category, product_id):
    dynamodb = boto3.resource("dynamodb", region_name=settings.S3_REGION)
    table = dynamodb.Table(category)

    try:
        res = table.get_item(Key={"product_id": product_id})
        item = res.get("Item")

        if not item:
            messages.error(request, "Product not found.")
            return redirect("admin_manage_products")

        image_key = item.get("image")

    except Exception as e:
        print("Error fetching product:", e)
        messages.error(request, "Unable to fetch product.")
        return redirect("admin_manage_products")

    ensure_bucket_exists()

    if image_key:
        try:
            s3 = boto3.client("s3", region_name=settings.S3_REGION)
            s3.delete_object(Bucket=settings.S3_BUCKET, Key=image_key)
            print(f"Deleted S3 Image: {image_key}")
        except Exception as e:
            print("S3 delete failed:", e)
            messages.warning(request, "Product deleted, but image could not be removed.")

    try:
        table.delete_item(Key={"product_id": product_id})
        messages.success(request, "Product removed successfully!")
    except Exception as e:
        print("DynamoDB delete failed:", e)
        messages.error(request, "Failed to delete item from database.")

    return redirect("admin_manage_products")


# =========================
# ANALYTICS HELPERS
# =========================
def decimal_to_float(obj):
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def export_orders_to_json():
    os.makedirs("analytics_data", exist_ok=True)

    dynamodb = boto3.resource("dynamodb", region_name=settings.S3_REGION)
    table = dynamodb.Table("Orders")

    response = table.scan()
    orders = response.get("Items", [])

    clean_orders = [decimal_to_float(order) for order in orders]

    path = "analytics_data/orders.json"

    with open(path, "w") as f:
        for order in clean_orders:
            f.write(json.dumps(order) + "\n")

    print(f"✅ Exported {len(clean_orders)} orders to {path}")
    return path


def free_memory_cache():
    """
    Drops Linux page/buff/cache before Spark runs so there
    is enough free RAM on the t2.micro instance.
    Requires ec2-user to have passwordless sudo for this command.
    Add to /etc/sudoers:
        ec2-user ALL=(ALL) NOPASSWD: /bin/sh -c sync*
    """
    try:
        subprocess.run(
            ["sudo", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
            check=True,
            timeout=10
        )
        print("✅ Memory cache cleared")
    except Exception as e:
        # Non-fatal — Spark may still have enough memory
        print(f"⚠️  Cache clear skipped: {e}")


def run_spark_job():
    try:
        spark_path = os.path.expanduser("~/environment/spark-3.5.1-bin-hadoop3/bin/spark-submit")

        # Free up Linux buffer/cache so Spark has enough RAM
        free_memory_cache()

        subprocess.run(
            [spark_path, "spark_jobs/weekly_sales_analytics.py"],
            check=True
        )

        print("✅ Spark job executed successfully")
        return True

    except Exception as e:
        print("❌ Spark job failed:", e)
        return False


def read_spark_output():
    path = "analytics_data/weekly_sales_output.json"

    if not os.path.exists(path):
        print("❌ Spark output file not found")
        return None

    try:
        with open(path, "r") as f:
            data = json.load(f)
            print("✅ Spark output loaded successfully")
            return data
    except Exception as e:
        print("❌ Failed reading Spark output:", e)
        return None


# =========================
# LIVE WEEKLY SALES DASHBOARD
# =========================
@admin_required
def admin_sales_dashboard(request):
    try:
        # STEP 1 → Export latest orders
        export_orders_to_json()

        # STEP 2 → Run Spark analytics
        spark_ok = run_spark_job()

        if not spark_ok:
            messages.error(request, "Spark analytics failed to run.")
            return render(request, "admin/admin_sales_dashboard.html", {
                "total_orders": 0,
                "total_revenue": 0,
                "total_items_sold": 0,
                "top_category": "N/A",
                "daily_sales": [],
                "category_sales": [],
                "top_products": [],
            })

        # STEP 3 → Read Spark result
        analytics = read_spark_output()

        if not analytics:
            messages.error(request, "No analytics data generated.")
            return render(request, "admin/admin_sales_dashboard.html", {
                "total_orders": 0,
                "total_revenue": 0,
                "total_items_sold": 0,
                "top_category": "N/A",
                "daily_sales": [],
                "category_sales": [],
                "top_products": [],
            })

        # STEP 4 → Render dashboard
        context = {
            "total_orders": analytics.get("total_orders", 0),
            "total_revenue": analytics.get("total_revenue", 0),
            "total_items_sold": analytics.get("total_items_sold", 0),
            "top_category": analytics.get("top_category", "N/A"),
            "daily_sales": analytics.get("daily_sales", []),
            "category_sales": analytics.get("category_sales", []),
            "top_products": analytics.get("top_products", []),
        }

        return render(request, "admin/admin_sales_dashboard.html", context)

    except Exception as e:
        print("❌ Weekly analytics dashboard failed:", e)
        messages.error(request, "Failed to generate weekly sales analytics.")

        return render(request, "admin/admin_sales_dashboard.html", {
            "total_orders": 0,
            "total_revenue": 0,
            "total_items_sold": 0,
            "top_category": "N/A",
            "daily_sales": [],
            "category_sales": [],
            "top_products": [],
        })