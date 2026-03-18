import boto3
import json
import os
from botocore.exceptions import ClientError

CONFIG_PATH = "infra/config.json"


# Config helpers

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=4)
    print(" Updated config.json")


# DynamoDB helpers

def create_cart_table(region):
    dynamodb = boto3.client("dynamodb", region_name=region)
    table_name = "UserCart"

    try:
        dynamodb.describe_table(TableName=table_name)
        print("✔ DynamoDB table already exists:", table_name)
        return table_name
    except ClientError:
        pass

    print(" Creating DynamoDB table: UserCart")

    dynamodb.create_table(
        TableName=table_name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "item_id", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "item_id", "KeyType": "RANGE"},
        ],
    )

    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=table_name)

    print(" UserCart table created")
    return table_name


def create_orders_table(region):
    dynamodb = boto3.client("dynamodb", region_name=region)
    table_name = "Orders"

    try:
        dynamodb.describe_table(TableName=table_name)
        print(" DynamoDB table already exists:", table_name)
        return table_name
    except ClientError:
        pass

    print(" Creating DynamoDB table: Orders")

    dynamodb.create_table(
        TableName=table_name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "order_id", "AttributeType": "S"}
        ],
        KeySchema=[
            {"AttributeName": "order_id", "KeyType": "HASH"}
        ]
    )

    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=table_name)

    print(" Orders table created")
    return table_name


# SNS helpers

def create_sns_topic(region):
    sns = boto3.client("sns", region_name=region)
    resp = sns.create_topic(Name="EasyCartOrderNotifications")
    print("✔ SNS Topic:", resp["TopicArn"])
    return resp["TopicArn"]


def subscribe_email_to_sns(region, topic_arn, email):
    sns = boto3.client("sns", region_name=region)

    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=email,
        ReturnSubscriptionArn=False
    )

    print(f" Subscription request sent to {email}")
    print(" NOTE: Email owner must click confirm link once (AWS requirement)")


# =========================
# Lambda helpers
# =========================

def create_lambda(region, fn_name, file_path, sns_topic_arn=None):

    import zipfile
    import io

    lambda_client = boto3.client("lambda", region_name=region)
    role_arn = "arn:aws:iam::928302362931:role/LabRole"

    # If this lambda needs SNS injection (place_order only)
    if sns_topic_arn:
        lambda_folder = file_path.replace(".zip", "")
        lambda_py = os.path.join(lambda_folder, "lambda_function.py")
        with open(lambda_py) as f:
            code_text = f.read()

        # Inject SNS topic
        code_text = code_text.replace("{{SNS_TOPIC_ARN}}", sns_topic_arn)

        # Create ZIP in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("lambda_function.py", code_text)
        zip_bytes = zip_buffer.getvalue()

    else:
        with open(file_path, "rb") as f:
            zip_bytes = f.read()

    # Deploy lambda (update or create)
    try:
        lambda_client.get_function(FunctionName=fn_name)
        print(f" Lambda exists: {fn_name}")

        lambda_client.update_function_code(
            FunctionName=fn_name,
            ZipFile=zip_bytes,
            Publish=True
        )
        print(f" Updated Lambda code for {fn_name}")
        return fn_name

    except lambda_client.exceptions.ResourceNotFoundException:
        print(f" Creating Lambda: {fn_name}")
        lambda_client.create_function(
            FunctionName=fn_name,
            Runtime="python3.9",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Timeout=15,
            Publish=True
        )
        print(f" Lambda created: {fn_name}")
        return fn_name


def update_lambda_env(region, fn_name, topic_arn):
    lambda_client = boto3.client("lambda", region_name=region)

    # Wait until Lambda update is finished
    waiter = lambda_client.get_waiter("function_updated")
    waiter.wait(FunctionName=fn_name)

    lambda_client.update_function_configuration(
        FunctionName=fn_name,
        Environment={
            "Variables": {
                "SNS_TOPIC_ARN": topic_arn
            }
        }
    )

    print(f" Environment variable added to {fn_name}")


# (Optional) Function URL helper – unused now, but kept if you ever want it.
def enable_function_url(fn_name, region):
    client = boto3.client("lambda", region_name=region)

    good_cors = {
        "AllowMethods": ["*"],
        "AllowOrigins": ["*"],
        "AllowHeaders": ["*"]
    }

    empty_cors = {
        "AllowMethods": [],
        "AllowOrigins": [],
        "AllowHeaders": []
    }

    print(f" Resetting corrupted CORS for {fn_name}...")

    try:
        client.delete_function_url_config(FunctionName=fn_name)
    except client.exceptions.ResourceNotFoundException:
        pass

    resp = client.create_function_url_config(
        FunctionName=fn_name,
        AuthType="NONE",
        Cors=empty_cors
    )

    resp = client.update_function_url_config(
        FunctionName=fn_name,
        AuthType="NONE",
        Cors=good_cors
    )

    url = resp["FunctionUrl"]

    try:
        client.add_permission(
            FunctionName=fn_name,
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
            StatementId=f"{fn_name}-public"
        )
    except ClientError as e:
        if "already exists" in str(e):
            print(f" Permission already exists for {fn_name}")
        else:
            raise e

    print(f" Function URL Enabled → {url}")
    return url


# =========================
# Main entrypoint
# =========================

def main():
    print("\n Setting up CART SERVICE (DynamoDB + Lambdas + SNS)\n")

    cfg = load_config()
    region = cfg["region"]

    #  Create DynamoDB tables
    cart_table = create_cart_table(region)
    cfg["cart_table"] = cart_table

    orders_table = create_orders_table(region)
    cfg["orders_table"] = orders_table

    #  SNS topic + subscription
    topic_arn = create_sns_topic(region)
    cfg["sns_topic_arn"] = topic_arn
    save_config(cfg)

    email_to_subscribe = "melwinpintoir@gmail.com"
    subscribe_email_to_sns(region, topic_arn, email_to_subscribe)

    #  Deploy Lambda functions (no API Gateway wiring here)
    lambdas = {
        "add_to_cart": "infra/lambda/add_to_cart.zip",
        "view_cart": "infra/lambda/view_cart.zip",
        "remove_cart_item": "infra/lambda/remove_cart_item.zip",
        "place_order": "infra/lambda/place_order.zip",
    }

    for name, path in lambdas.items():
        fn_name = f"EasyCart_{name}"

        if name == "place_order":
            create_lambda(region, fn_name, path, sns_topic_arn=topic_arn)
            update_lambda_env(region, fn_name, topic_arn)
        else:
            create_lambda(region, fn_name, path)


    save_config(cfg)

    print("\n🎉 CART SERVICE READY (tables + lambdas + SNS)!\n")


if __name__ == "__main__":
    main()
