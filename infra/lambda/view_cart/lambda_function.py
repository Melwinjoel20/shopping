import json
import boto3
from boto3.dynamodb.conditions import Attr
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("UserCart")


def clean_decimal(obj):
    """Convert DynamoDB Decimal types to float for JSON serialization."""
    if isinstance(obj, list):
        return [clean_decimal(i) for i in obj]
    if isinstance(obj, dict):
        return {k: clean_decimal(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Content-Type": "application/json",
    }


def lambda_handler(event, context):
    print("Incoming event:", json.dumps(event))

    # Detect HTTP method
    method = (
        (event.get("requestContext", {}) or {})
        .get("http", {})
        .get("method")
        or event.get("httpMethod", "")
    ).upper()

    # Handle preflight
    if method == "OPTIONS":
        return {
            "statusCode": 204,
            "headers": _cors_headers(),
            "body": ""
        }

    # ✅ Read user_id from query params safely
    query_params = event.get("queryStringParameters") or {}
    user_id = query_params.get("user_id")

    print("QUERY PARAMS:", query_params)
    print("FINAL USER_ID:", user_id)

    if not user_id:
        return {
            "statusCode": 400,
            "headers": _cors_headers(),
            "body": json.dumps({"message": "Missing user_id"})
        }

    # Fetch cart items for that user
    response = table.scan(
        FilterExpression=Attr("user_id").eq(user_id)
    )

    items = response.get("Items", [])
    items = clean_decimal(items)

    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps(items)
    }