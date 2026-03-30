import json
import boto3
import uuid
from decimal import Decimal

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

    # -----------------------------------------------------
    # Detect HTTP method
    # -----------------------------------------------------
    method = (
        (event.get("requestContext", {}) or {})
        .get("http", {})
        .get("method")
        or event.get("httpMethod", "")
    ).upper()

    # CORS preflight
    if method == "OPTIONS":
        return {
            "statusCode": 204,
            "headers": _cors_headers(),
            "body": ""
        }

    # -----------------------------------------------------
    # Extract JWT claims
    # -----------------------------------------------------
    auth = event.get("requestContext", {}).get("authorizer", {})
    claims = auth.get("jwt", {}).get("claims", {}) or auth.get("claims", {})

    print("JWT Claims:", claims)

    # -----------------------------------------------------
    # Parse request body safely
    # -----------------------------------------------------
    if "body" in event:
        try:
            body = json.loads(event.get("body") or "{}")
        except Exception:
            body = {}
    else:
        body = event  # for testing in Lambda console

    print("Parsed body:", body)

    # -----------------------------------------------------
    # Determine user_id
    # -----------------------------------------------------
    user_id = (
        claims.get("email")
        or claims.get("cognito:username")
        or body.get("user_id")
    )

    if not user_id:
        return {
            "statusCode": 401,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Unauthorized: Please log in"})
        }

    # -----------------------------------------------------
    # Extract product details
    # -----------------------------------------------------
    product_id = body.get("product_id")
    name = body.get("name")
    price = body.get("price")
    image = body.get("image")
    category = body.get("category", "General")

    # -----------------------------------------------------
    # Validate required fields
    # -----------------------------------------------------
    if not product_id or not name or price is None:
        return {
            "statusCode": 400,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Missing required product fields"})
        }

    # -----------------------------------------------------
    # Create cart item
    # -----------------------------------------------------
    item_id = str(uuid.uuid4())

    item = {
        "user_id": user_id,
        "item_id": item_id,
        "product_id": product_id,
        "name": name,
        "price": Decimal(str(price)),
        "image": image,
        "qty": 1,
        "category": category
    }

    print("Saving item:", item)

    # -----------------------------------------------------
    # Save to DynamoDB
    # -----------------------------------------------------
    try:
        table.put_item(Item=item)
    except Exception as e:
        print("DynamoDB Error:", e)
        return {
            "statusCode": 500,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Failed to add to cart"})
        }

    # -----------------------------------------------------
    # Success response
    # -----------------------------------------------------
    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps({
            "message": "Added to cart successfully",
            "item_id": item_id
        })
    }