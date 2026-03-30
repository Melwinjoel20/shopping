import json
import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("UserCart")

def _cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
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
        return {"statusCode": 204, "headers": _cors_headers(), "body": ""}

    body = json.loads(event.get("body") or "{}")

    user_id = body.get("user_id")
    item_id = body.get("item_id")

    print("DELETE user_id:", user_id)
    print("DELETE item_id:", item_id)

    if not user_id or not item_id:
        return {
            "statusCode": 400,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Missing user_id or item_id"})
        }

    # ✅ DIRECT DELETE (no scan needed)
    table.delete_item(
        Key={
            "user_id": user_id,
            "item_id": item_id
        }
    )

    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps({"message": "Item removed"})
    }
    