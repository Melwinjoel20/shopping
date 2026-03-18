from decimal import Decimal
import boto3
import uuid
from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib import messages
from .views import admin_required


# ADMIN DASHBOARD
@admin_required
def admin_dashboard(request):
    return render(request, "admin/admin_dashboard.html")



# HELPER: ENSURE BUCKET EXISTS
def ensure_bucket_exists():
    s3 = boto3.client("s3", region_name=settings.S3_REGION)
    bucket_name = settings.S3_BUCKET

    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        # Bucket missing → create bucket
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



# HELPER: UPLOAD FILE TO S3
def upload_product_image_to_s3(file_obj):
    """
    Uploads a product image to S3.
    Auto-creates the bucket if missing.
    Returns the S3 key if successful.
    """
    s3 = boto3.client("s3", region_name=settings.S3_REGION)
    bucket_name = settings.S3_BUCKET

    # Ensure bucket exists
    if not ensure_bucket_exists():
        return None

    # Create unique filename
    ext = file_obj.name.split(".")[-1]
    unique_name = f"product-images/{uuid.uuid4()}.{ext}"

    # Upload to S3
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



# ADD PRODUCT
@admin_required
def admin_add_product(request):
    if request.method == "POST":
        category = request.POST.get("category")
        name = request.POST.get("name")
        description = request.POST.get("description")
        price = Decimal(request.POST.get("price"))
        image_file = request.FILES.get("image_file")

        if not image_file:
            messages.error(request, "Please upload an image.")
            return redirect("admin_add_product")

        # Upload image to S3
        s3_key = upload_product_image_to_s3(image_file)
        if not s3_key:
            messages.error(request, "Failed to upload image to S3.")
            return redirect("admin_add_product")

        # Save product in DynamoDB
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

    return render(request, "admin/add_product.html")



# VIEW / LIST ALL PRODUCTS
@admin_required
def admin_manage_products(request):
    dynamodb = boto3.resource("dynamodb", region_name=settings.S3_REGION)
    categories = ["Phones", "Laptops", "Accessories"]

    products = []

    for cat in categories:
        table = dynamodb.Table(cat)
        res = table.scan().get("Items", [])
        for item in res:
            item["category"] = cat
            products.append(item)

    return render(request, "admin/manage_products.html", {"products": products})



# DELETE PRODUCT (DELETE FROM S3 + DYNAMODB)
@admin_required
def admin_delete_product(request, category, product_id):
    dynamodb = boto3.resource("dynamodb", region_name=settings.S3_REGION)
    table = dynamodb.Table(category)

    # 1. Fetch product
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

    # 2. Delete image from S3 (Bucket auto-created if needed)
    ensure_bucket_exists()

    if image_key:
        try:
            s3 = boto3.client("s3", region_name=settings.S3_REGION)
            s3.delete_object(Bucket=settings.S3_BUCKET, Key=image_key)
            print(f"Deleted S3 Image: {image_key}")
        except Exception as e:
            print("S3 delete failed:", e)
            messages.warning(request, "Product deleted, but image could not be removed.")

    # 3. Delete DynamoDB record
    try:
        table.delete_item(Key={"product_id": product_id})
        messages.success(request, "Product removed successfully!")
    except Exception as e:
        print("DynamoDB delete failed:", e)
        messages.error(request, "Failed to delete item from database.")

    return redirect("admin_manage_products")
