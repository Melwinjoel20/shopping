import boto3
from botocore.exceptions import ClientError

# =========================
# CONFIG
# =========================
REGION = "us-east-1"   # change if needed
TABLE_NAME = "SalesAnalyticsWeekly"


# =========================
# CHECK IF TABLE EXISTS
# =========================
def table_exists(client, table_name):
    try:
        client.describe_table(TableName=table_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise e


# =========================
# CREATE TABLE
# =========================
def create_table():
    dynamodb = boto3.client("dynamodb", region_name=REGION)

    if table_exists(dynamodb, TABLE_NAME):
        print(f"✔ Table already exists: {TABLE_NAME}")
        return

    print(f"🛠 Creating table: {TABLE_NAME}")

    dynamodb.create_table(
        TableName=TABLE_NAME,
        AttributeDefinitions=[
            {
                "AttributeName": "report_id",
                "AttributeType": "S"
            }
        ],
        KeySchema=[
            {
                "AttributeName": "report_id",
                "KeyType": "HASH"
            }
        ],
        BillingMode="PAY_PER_REQUEST"
    )

    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=TABLE_NAME)

    print(f"✔ Table created successfully: {TABLE_NAME}")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    create_table()