import json
import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("UserCart")


def _cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Content-Type": "application/json",
    }


def lambda_handler(event, context):
    print("Incoming event:", json.dumps(event))

    method = (
        event.get("requestContext", {})
             .get("http", {})
             .get("method")
        or event.get("httpMethod", "")
    ).upper()

    if method == "OPTIONS":
        return {
            "statusCode": 204,
            "headers": _cors_headers(),
            "body": ""
        }

    #  Extract user from JWT
    auth = event.get("requestContext", {}).get("authorizer", {})

    claims = auth.get("jwt", {}).get("claims", {}) or auth.get("claims", {})

    user_id = claims.get("email") or claims.get("cognito:username")

    if not user_id:
        return {
            "statusCode": 401,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Unauthorized: Please log in"})
        }
    # -------------------------------

    body_str = event.get("body") or "{}"
    try:
        body = json.loads(body_str)
    except:
        body = {}

    item_id = body.get("item_id")

    if not item_id:
        return {
            "statusCode": 400,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "item_id is required"})
        }

    #  Scaningn the item because item_id is NOT partition key
    response = table.scan(
        FilterExpression=Attr("item_id").eq(item_id)
                     & Attr("user_id").eq(user_id)  # 🔒 ensure item belongs to THIS user
    )

    items = response.get("Items", [])

    if not items:
        return {
            "statusCode": 404,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Item not found"})
        }

    cart_item = items[0]

    table.delete_item(
        Key={
            "user_id": cart_item["user_id"],
            "item_id": cart_item["item_id"]
        }
    )

    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps({"message": "Item removed"})
    }
