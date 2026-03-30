from decimal import Decimal
import boto3
import uuid
import time
from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib import messages
from .views import admin_required
import json


# =========================
# CONFIG
# =========================
REGION     = settings.S3_REGION
BUCKET     = settings.S3_BUCKET
GLUE_JOB   = "weekly-sales-analytics"
OUTPUT_KEY = "analytics/weekly_sales_output.json"


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
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        try:
            s3.create_bucket(
                Bucket=BUCKET,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )
            print(f"Bucket '{BUCKET}' created successfully.")
        except Exception as e:
            print("Bucket creation failed:", e)
            return False
    return True


# =========================
# HELPER: UPLOAD FILE TO S3
# =========================
def upload_product_image_to_s3(file_obj):
    s3 = boto3.client("s3", region_name=REGION)

    if not ensure_bucket_exists():
        return None

    ext         = file_obj.name.split(".")[-1]
    unique_name = f"product-images/{uuid.uuid4()}.{ext}"

    try:
        s3.upload_fileobj(
            file_obj,
            BUCKET,
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
        category    = request.POST.get("category")
        name        = request.POST.get("name")
        description = request.POST.get("description")
        price       = Decimal(request.POST.get("price"))
        image_file  = request.FILES.get("image_file")

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

        dynamodb = boto3.resource("dynamodb", region_name=REGION)
        table    = dynamodb.Table(category)
        pid      = str(uuid.uuid4())

        table.put_item(Item={
            "product_id":  pid,
            "name":        name,
            "description": description,
            "price":       price,
            "image":       s3_key
        })

        messages.success(request, "Product added successfully!")
        return redirect("admin_dashboard")

    return render(request, "admin/add_product.html", {"categories": categories})


# =========================
# VIEW / LIST ALL PRODUCTS
# =========================
@admin_required
def admin_manage_products(request):
    dynamodb   = boto3.resource("dynamodb", region_name=REGION)
    categories = settings.COGNITO.get("dynamodb_tables", [])
    products   = []

    for cat in categories:
        table = dynamodb.Table(cat)
        res   = table.scan().get("Items", [])
        for item in res:
            item["category"] = cat
            products.append(item)

    return render(request, "admin/manage_products.html", {
        "products":   products,
        "categories": categories
    })


# =========================
# DELETE PRODUCT
# =========================
@admin_required
def admin_delete_product(request, category, product_id):
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table    = dynamodb.Table(category)

    try:
        res  = table.get_item(Key={"product_id": product_id})
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
            s3 = boto3.client("s3", region_name=REGION)
            s3.delete_object(Bucket=BUCKET, Key=image_key)
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
# GLUE: TRIGGER JOB
# =========================
def run_glue_job():
    try:
        glue = boto3.client("glue", region_name=REGION)

        # Check if job is already running — avoid ConcurrentRunsExceededException
        runs = glue.get_job_runs(JobName=GLUE_JOB, MaxResults=1)
        if runs["JobRuns"] and runs["JobRuns"][0]["JobRunState"] in ("RUNNING", "STARTING"):
            run_id = runs["JobRuns"][0]["Id"]
            print(f"⏳ Glue job already running — reusing RunId: {run_id}")
        else:
            response = glue.start_job_run(JobName=GLUE_JOB)
            run_id   = response["JobRunId"]
            print(f"✅ Glue job started — RunId: {run_id}")

        # Poll until complete
        max_wait   = 600
        poll_every = 15
        elapsed    = 0

        while elapsed < max_wait:
            time.sleep(poll_every)
            elapsed    += poll_every
            status      = glue.get_job_run(JobName=GLUE_JOB, RunId=run_id)
            state       = status["JobRun"]["JobRunState"]
            print(f"⏳ Glue state: {state} ({elapsed}s)")

            if state == "SUCCEEDED":
                print("✅ Glue job completed")
                return True

            if state in ("FAILED", "ERROR", "TIMEOUT", "STOPPED"):
                error = status["JobRun"].get("ErrorMessage", "No details")
                print(f"❌ Glue job failed — {state}: {error}")
                return False

        print("❌ Glue job timed out")
        return False

    except Exception as e:
        print(f"❌ Failed to trigger Glue job: {e}")
        return False


# =========================
# GLUE: READ OUTPUT FROM S3
# =========================
def read_glue_output():
    try:
        s3   = boto3.client("s3", region_name=REGION)
        obj  = s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        print("✅ Analytics output loaded from S3")
        return data
    except Exception as e:
        print(f"❌ Failed to read Glue output from S3: {e}")
        return None


# =========================
# WEEKLY SALES DASHBOARD
# =========================
@admin_required
def admin_sales_dashboard(request):

    empty_context = {
        "total_orders":     0,
        "total_revenue":    0,
        "total_items_sold": 0,
        "top_category":     "N/A",
        "daily_sales":      [],
        "category_sales":   [],
        "top_products":     [],
    }

    try:
        glue_ok = run_glue_job()

        if not glue_ok:
            messages.error(request, "Spark analytics (Glue) failed to run.")
            return render(request, "admin/admin_sales_dashboard.html", empty_context)

        analytics = read_glue_output()

        if not analytics:
            messages.error(request, "No analytics data found in S3.")
            return render(request, "admin/admin_sales_dashboard.html", empty_context)

        context = {
            "total_orders":     analytics.get("total_orders", 0),
            "total_revenue":    analytics.get("total_revenue", 0),
            "total_items_sold": analytics.get("total_items_sold", 0),
            "top_category":     analytics.get("top_category", "N/A"),
            "daily_sales":      analytics.get("daily_sales", []),
            "category_sales":   analytics.get("category_sales", []),
            "top_products":     analytics.get("top_products", []),
        }

        return render(request, "admin/admin_sales_dashboard.html", context)

    except Exception as e:
        print(f"❌ Weekly analytics dashboard failed: {e}")
        messages.error(request, "Failed to generate weekly sales analytics.")
        return render(request, "admin/admin_sales_dashboard.html", empty_context)