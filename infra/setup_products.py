import boto3
import os
import json
import uuid
from botocore.exceptions import ClientError

CONFIG_PATH = "infra/config.json"


# CONFIG HELPERS
def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def save_config(data):
    with open(CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=4)
    print("✔ config.json updated")


# S3 FUNCTIONS — Upload product images
def upload_product_images(bucket, region, folder):
    s3 = boto3.client("s3", region_name=region)

    uploaded = {}

    for file_name in os.listdir(folder):
        if not file_name.lower().endswith((".png", ".jpg", ".jpeg")):
            continue

        local_path = os.path.join(folder, file_name)
        s3_key = f"product-images/{file_name}"

        s3.upload_file(
            local_path,
            bucket,
            s3_key,
            ExtraArgs={"ContentType": "image/jpeg"}
        )

        # store ONLY the S3 key, not a URL
        uploaded[file_name] = s3_key
        print(f"✔ Uploaded {file_name} → key = {s3_key}")

    return uploaded



# DYNAMODB FUNCTIONS
def table_exists(client, table_name):
    try:
        client.describe_table(TableName=table_name)
        return True
    except ClientError:
        return False


def create_table(region, table_name):
    client = boto3.client("dynamodb", region_name=region)

    if table_exists(client, table_name):
        print(f"✔ Table exists: {table_name}")
        return

    print(f"🛠 Creating table → {table_name}")

    client.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "product_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "product_id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST"
    )

    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)

    print(f"✔ Created DynamoDB table → {table_name}")


def seed_table(region, table_name, products):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    print(f"📝 Seeding table → {table_name}")

    for p in products:
        pid = str(uuid.uuid4())

        table.put_item(
            Item={
                "product_id": pid,
                "name": p["name"],
                "description": p["description"],
                "price": p["price"],
                "image": p.get("image", "")
            }
        )

        print(f"✔ Added: {p['name']} → {pid}")


def update_images(region, table_name, mapping):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    items = table.scan().get("Items", [])

    for item in items:
        name_key = item["name"].replace(" ", "").lower()

        for file_name, key in mapping.items():
            if name_key in file_name.replace(" ", "").lower():
                table.update_item(
                    Key={"product_id": item["product_id"]},
                    UpdateExpression="SET image = :u",
                    ExpressionAttributeValues={":u": key}  # now S3 key
                )
                print(f"✔ Updated image key → {item['name']}")
                break


# MAIN ORCHESTRATOR
def main():
    config = load_config()

    region = config["region"]
    bucket = config["bucket_name"]
    tables = config["dynamodb_tables"]
    images_folder = config["product_images_folder"]

    print("\n🚀 Starting EasyCart PRODUCT SETUP\n")

    #  Upload product images
    uploaded_mapping = upload_product_images(bucket, region, images_folder)

    # Create + Seed DynamoDB tables
    SAMPLE_DATA = {
        "Phones": [
            {"name": "iPhone 14 Pro", "description": "A16 Bionic chip", "price": 1099},
            {"name": "Samsung Galaxy S23", "description": "AMOLED Display", "price": 999}
        ],
        "Laptops": [
            {"name": "MacBook Air M2", "description": "Apple M2", "price": 1199},
            {"name": "Dell XPS 13", "description": "Intel i7 13th Gen", "price": 1399}
        ],
        "Accessories": [
            {"name": "AirPods Pro", "description": "Noise Cancellation", "price": 249},
            {"name": "Logitech MX Master 3", "description": "Wireless mouse", "price": 99}
        ]
    }

    for table in tables:
        create_table(region, table)
        seed_table(region, table, SAMPLE_DATA[table])
        update_images(region, table, uploaded_mapping)

    print("\n🎉 ALL DONE — Products + Images + Tables set up correctly!\n")


if __name__ == "__main__":
    main()
