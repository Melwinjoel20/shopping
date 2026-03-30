from decimal import Decimal
import boto3
import uuid
from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import JsonResponse
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

    ext = file_obj.name.split(".")[-1]
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

        dynamodb = boto3.resource("dynamodb", region_name=REGION)
        table = dynamodb.Table(category)
        pid = str(uuid.uuid4())

        table.put_item(Item={
            "product_id": pid,
            "name": name,
            "description": description,
            "price": price,
            "image": s3_key
        })

        messages.success(request, "Product added successfully!")
        return redirect("admin_dashboard")

    return render(request, "admin/add_product.html", {"categories": categories})


# =========================
# VIEW / LIST ALL PRODUCTS
# =========================
@admin_required
def admin_manage_products(request):
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
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
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
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
# GLUE: READ OUTPUT FROM S3
# =========================
def read_glue_output():
    try:
        s3 = boto3.client("s3", region_name=REGION)
        obj = s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        print("✅ Analytics output loaded from S3")
        return data
    except Exception as e:
        print(f"❌ Failed to read Glue output from S3: {e}")
        return None


# =========================
# WEEKLY SALES DASHBOARD — FAST ASYNC VERSION
# =========================

@admin_required
def admin_sales_dashboard(request):
    """
    Renders the dashboard shell immediately.
    Frontend JS will trigger Glue + poll + fetch data.
    """
    return render(request, "admin/admin_sales_dashboard.html")


@admin_required
def admin_sales_trigger(request):
    """
    POST — fires Glue and returns RunId immediately.
    No waiting here.
    """
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    try:
        glue = boto3.client("glue", region_name=REGION)

        runs = glue.get_job_runs(JobName=GLUE_JOB, MaxResults=1)

        if runs["JobRuns"] and runs["JobRuns"][0]["JobRunState"] in ("RUNNING", "STARTING"):
            run_id = runs["JobRuns"][0]["Id"]
            print(f"⏳ Glue already running — reusing RunId: {run_id}")
        else:
            run_id = glue.start_job_run(JobName=GLUE_JOB)["JobRunId"]
            print(f"✅ Glue started — RunId: {run_id}")

        return JsonResponse({
            "run_id": run_id,
            "status": "started"
        })

    except Exception as e:
        print(f"❌ Failed to trigger Glue job: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@admin_required
def admin_sales_status(request):
    """
    GET ?run_id=xxx — checks Glue job state.
    Frontend should call this every ~10 seconds.
    """
    run_id = request.GET.get("run_id")

    if not run_id:
        return JsonResponse({"error": "run_id required"}, status=400)

    try:
        glue = boto3.client("glue", region_name=REGION)
        state = glue.get_job_run(
            JobName=GLUE_JOB,
            RunId=run_id
        )["JobRun"]["JobRunState"]

        return JsonResponse({"state": state})

    except Exception as e:
        print(f"❌ Failed to check Glue status: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@admin_required
def admin_sales_data(request):
    """
    GET — reads final Glue output JSON from S3.
    Frontend should call this only after state == SUCCEEDED.
    """
    try:
        data = read_glue_output()

        if not data:
            return JsonResponse({"error": "No output found in S3"}, status=404)

        return JsonResponse(data)

    except Exception as e:
        print(f"❌ Failed to return analytics data: {e}")
        return JsonResponse({"error": str(e)}, status=500)