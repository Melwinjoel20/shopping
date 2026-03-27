from decimal import Decimal
import boto3
import uuid
from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib import messages
from .views import admin_required
from datetime import datetime, timedelta



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
    


from datetime import datetime, timedelta
import boto3
from django.conf import settings
from django.shortcuts import render
from .views import admin_required


@admin_required
def admin_sales_dashboard(request):
    dynamodb = boto3.resource("dynamodb", region_name=settings.S3_REGION)
    table = dynamodb.Table("Orders")

    response = table.scan()
    orders = response.get("Items", [])

    # last 7 days
    now = datetime.utcnow()
    last_7_days = now - timedelta(days=7)

    total_orders = 0
    total_revenue = 0
    total_items_sold = 0

    daily_sales = {}
    category_sales = {}
    product_sales = {}

    for order in orders:
        created_at = order.get("created_at")
        if not created_at:
            continue

        try:
            order_date = datetime.fromisoformat(created_at)
        except:
            continue

        if order_date < last_7_days:
            continue

        total_orders += 1
        order_total = float(order.get("total_amount", 0))
        total_revenue += order_total

        day = order_date.strftime("%A")
        if day not in daily_sales:
            daily_sales[day] = {"orders": 0, "revenue": 0}

        daily_sales[day]["orders"] += 1
        daily_sales[day]["revenue"] += order_total

        # ✅ NEW item parsing
        items = order.get("items", [])

        for item in items:
            name = item.get("name", "Unknown")
            price = float(item.get("price", 0))
            qty = int(item.get("qty", 1))
            category = item.get("category", "General")

            total_items_sold += qty

            # Product sales
            if name not in product_sales:
                product_sales[name] = {"qty": 0, "revenue": 0}

            product_sales[name]["qty"] += qty
            product_sales[name]["revenue"] += price * qty

            # Category formatting
            if category == "MenClothes":
                category_label = "Men Clothes"
            elif category == "WomenClothes":
                category_label = "Women Clothes"
            elif category == "KidsClothes":
                category_label = "Kids Clothes"
            else:
                category_label = "General"

            if category_label not in category_sales:
                category_sales[category_label] = {"items": 0, "revenue": 0}

            category_sales[category_label]["items"] += qty
            category_sales[category_label]["revenue"] += price * qty

    # format for template
    daily_sales_list = [
        {"day": d, "orders": v["orders"], "revenue": round(v["revenue"], 2)}
        for d, v in daily_sales.items()
    ]

    category_sales_list = [
        {"category": k, "items_sold": v["items"], "revenue": round(v["revenue"], 2)}
        for k, v in category_sales.items()
    ]

    top_products_list = sorted(
        [
            {"name": k, "qty_sold": v["qty"], "revenue": round(v["revenue"], 2)}
            for k, v in product_sales.items()
        ],
        key=lambda x: x["qty_sold"],
        reverse=True
    )[:5]

    top_category = max(category_sales, key=lambda x: category_sales[x]["items"], default="N/A")

    context = {
        "total_orders": total_orders,
        "total_revenue": round(total_revenue, 2),
        "total_items_sold": total_items_sold,
        "top_category": top_category,
        "daily_sales": daily_sales_list,
        "category_sales": category_sales_list,
        "top_products": top_products_list,
    }

    return render(request, "admin/admin_sales_dashboard.html", context)