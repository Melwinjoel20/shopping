import boto3

def create_rate_limit_table(region="us-east-1"):
    dynamodb = boto3.client("dynamodb", region_name=region)
    table_name = "RateLimits"

    # Check if exists
    try:
        dynamodb.describe_table(TableName=table_name)
        print(f"Table already exists: {table_name}")
        return
    except:
        pass

    print(" Creating RateLimits table...")

    dynamodb.create_table(
        TableName=table_name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "key", "AttributeType": "S"}
        ],
        KeySchema=[
            {"AttributeName": "key", "KeyType": "HASH"}
        ]
    )

    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=table_name)

    # Enable TTL
    dynamodb.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={
            "Enabled": True,
            "AttributeName": "ttl"
        }
    )

    print(" RateLimits table created with TTL enabled")

if __name__ == "__main__":
    create_rate_limit_table("us-east-1")
