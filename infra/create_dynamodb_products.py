import boto3
import os
import json
import uuid
from botocore.exceptions import ClientError
from decimal import Decimal

CONFIG_PATH = "infra/config.json"


# =========================
# CONFIG
# =========================
def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


# =========================
# CATEGORY MAPPING
# =========================
CATEGORY_MAP = {
    "men": "MenClothes",
    "women": "WomenClothes",
    "kids": "KidsClothes"
}


# =========================
# PARSE PRODUCT FROM FILENAME
# =========================
def parse_product(file_name):
    name = file_name.rsplit(".", 1)[0]  # remove extension
    parts = name.split("_")

    if len(parts) < 3:
        print(f"⚠ Skipping invalid filename: {file_name}")
        return None

    category_key = parts[0].lower()
    price = parts[-1]

    if category_key not in CATEGORY_MAP:
        print(f"⚠ Unknown category: {file_name}")
        return None

    try:
        price = int(price)
    except:
        print(f"⚠ Invalid price in: {file_name}")
        return None

    product_words = parts[1:-1]
    product_name = " ".join(word.capitalize() for word in product_words)

    return {
        "category": CATEGORY_MAP[category_key],
        "name": product_name,
        "price": price,
        "description": f"Premium {product_name.lower()}",
        "image_file": file_name
    }


# =========================
# S3 UPLOAD (SKIP IF EXISTS)
# =========================
def upload_product_images(bucket, region, folder):
    s3 = boto3.client("s3", region_name=region)
    uploaded = {}

    for file_name in os.listdir(folder):
        if not file_name.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
            continue

        local_path = os.path.join(folder, file_name)
        s3_key = f"product-images/{file_name}"

        if file_name.endswith(".png"):
            content_type = "image/png"
        elif file_name.endswith(".webp"):
            content_type = "image/webp"
        else:
            content_type = "image/jpeg"

        try:
            s3.head_object(Bucket=bucket, Key=s3_key)
            print(f"⏭ Skipped (exists): {file_name}")

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                s3.upload_file(
                    local_path,
                    bucket,
                    s3_key,
                    ExtraArgs={"ContentType": content_type}
                )
                print(f"✔ Uploaded: {file_name}")
            else:
                raise e

        uploaded[file_name] = s3_key

    return uploaded


# =========================
# DYNAMODB HELPERS
# =========================
def create_table_if_needed(region, table_name):
    dynamodb = boto3.client("dynamodb", region_name=region)

    try:
        dynamodb.describe_table(TableName=table_name)
        print(f"✔ Table exists: {table_name}")
    except ClientError:
        print(f"🛠 Creating table: {table_name}")
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "product_id", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "product_id", "KeyType": "HASH"},
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=table_name)
        print(f"✔ Created table: {table_name}")


def insert_product(region, table_name, product, image_key):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    table.put_item(
        Item={
            "product_id": str(uuid.uuid4()),
            "name": product["name"],
            "description": product["description"],
            "price": product["price"],
            "image": image_key
        }
    )

    print(f"✔ Inserted: {product['name']} → {table_name}")


# =========================
# MAIN
# =========================
def main():
    config = load_config()

    region = config["region"]
    bucket = config["bucket_name"]
    folder = config["product_images_folder"]

    print("\n🚀 AUTO PRODUCT INGESTION STARTED\n")

    uploaded = upload_product_images(bucket, region, folder)

    for file_name in uploaded:
        product = parse_product(file_name)
        if not product:
            continue

        table_name = product["category"]
        create_table_if_needed(region, table_name)

        insert_product(
            region,
            table_name,
            product,
            uploaded[file_name]
        )

    print("\n🎉 AUTO IMPORT COMPLETE!\n")


if __name__ == "__main__":
    main()