import boto3
import json
import os
import uuid
from botocore.exceptions import ClientError

CONFIG_PATH = "infra/config.json"


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


def save_config(data):
    with open(CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=4)
    print(" config.json updated")


def table_exists(dynamodb, table_name):
    try:
        dynamodb.describe_table(TableName=table_name)
        return True
    except ClientError:
        return False


def create_table_if_needed(region, table_name):
    dynamodb = boto3.client("dynamodb", region_name=region)

    if table_exists(dynamodb, table_name):
        print(f" Table already exists: {table_name}")
        return

    print(f"\n[1] Creating table: {table_name} ...")

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

    print("Waiting for table to be ACTIVE...")
    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=table_name)

    print(f" Created table: {table_name}")


def seed_data(region, table_name, products):
    print(f"\n[2] Seeding products into {table_name}...")

    db = boto3.resource("dynamodb", region_name=region)
    table = db.Table(table_name)

    for p in products:
        p_id = str(uuid.uuid4())

        table.put_item(
            Item={
                "product_id": p_id,
                "name": p["name"],
                "description": p["description"],
                "price": p["price"],
                "image": p["image"]
            }
        )

        print(f" Added: {p['name']} → {p_id}")


def main():
    print(" EasyCart DynamoDB Setup — Multi-table Mode")

    config = load_config()
    region = config.get("region", "us-east-1")

    # 3 tables
    TABLES = {
        "Phones": [
            {
                "name": "iPhone 14 Pro",
                "description": "Apple A16 Bionic, 6.1-inch OLED Display",
                "price": 1099,
                "image": "https://via.placeholder.com/300"
            },
            {
                "name": "Samsung Galaxy S23",
                "description": "Snapdragon 8 Gen 2, Dynamic AMOLED",
                "price": 999,
                "image": "https://via.placeholder.com/300"
            }
        ],

        "Laptops": [
            {
                "name": "MacBook Air M2",
                "description": "Apple M2, 8GB RAM, 256GB SSD",
                "price": 1199,
                "image": "https://via.placeholder.com/300"
            },
            {
                "name": "Dell XPS 13",
                "description": "Intel i7 13th Gen, 16GB RAM",
                "price": 1399,
                "image": "https://via.placeholder.com/300"
            }
        ],

        "Accessories": [
            {
                "name": "AirPods Pro",
                "description": "Active Noise Cancellation, Spatial Audio",
                "price": 249,
                "image": "https://via.placeholder.com/300"
            },
            {
                "name": "Logitech MX Master 3",
                "description": "Advanced Wireless Mouse",
                "price": 99,
                "image": "https://via.placeholder.com/300"
            }
        ]
    }

    # Create + Seed each table
    for table_name, items in TABLES.items():
        create_table_if_needed(region, table_name)
        seed_data(region, table_name, items)

    print("\n All DynamoDB tables created + seeded successfully!")


if __name__ == "__main__":
    main()
