import boto3
import json
import os
import datetime
from botocore.exceptions import ClientError
from passlib.hash import pbkdf2_sha256

CONFIG_PATH = "infra/config.json"
ADMIN_TABLE_KEY = "admin_users_table"
DEFAULT_ADMIN_TABLE_NAME = "EasyCartAdminUsers"


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


def save_config(data):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=4)
    print(" config.json updated")


def table_exists(dynamodb, table_name):
    try:
        dynamodb.describe_table(TableName=table_name)
        return True
    except ClientError:
        return False


def create_admin_table_if_needed(region, table_name):
    dynamodb = boto3.client("dynamodb", region_name=region)

    if table_exists(dynamodb, table_name):
        print(f" Table already exists: {table_name}")
        return

    print(f"\n Creating table: {table_name} ...")

    dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "email", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "email", "KeyType": "HASH"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    print("⏳ Waiting for table to be ACTIVE...")
    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=table_name)

    print(f" Created table: {table_name}")


def create_admin_user(region, table_name, email, password, role="SUPER_ADMIN"):
    """
    Creates an admin user in DynamoDB with a hashed password.
    """
    db = boto3.resource("dynamodb", region_name=region)
    table = db.Table(table_name)

    password_hash = pbkdf2_sha256.hash(password)

    item = {
        "email": email,
        "password_hash": password_hash,
        "role": role,  # "ADMIN" or "SUPER_ADMIN"
        "is_active": True,
        "created_at": datetime.datetime.utcnow().isoformat(),
    }

    table.put_item(Item=item)
    print(f" Added admin user: {email} ({role})")


def main():
    print(" EasyCart DynamoDB Setup — Admin Users")

    config = load_config()
    region = config.get("region", "us-east-1")

    # Choose table name (can override in config if needed)
    table_name = config.get(ADMIN_TABLE_KEY, DEFAULT_ADMIN_TABLE_NAME)

    # 1) Create table if needed
    create_admin_table_if_needed(region, table_name)

    # Save table name back to config for future reference
    config[ADMIN_TABLE_KEY] = table_name
    save_config(config)

    # 2) Seed one admin user (interactive)
    print("\n[2] Create initial admin user")
    email = input("Admin email: ").strip()
    if not email:
        print(" Email is required. Exiting.")
        return

    password = input("Admin password: ").strip()
    if not password:
        print(" Password is required. Exiting.")
        return

    role = input("Role (ADMIN/SUPER_ADMIN) [SUPER_ADMIN]: ").strip().upper() or "SUPER_ADMIN"
    if role not in ("ADMIN", "SUPER_ADMIN"):
        print("Invalid role, defaulting to ADMIN")
        role = "ADMIN"

    create_admin_user(region, table_name, email, password, role)

    print("\n Admin users table ready and initial user created!")


if __name__ == "__main__":
    main()
