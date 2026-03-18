import json
import boto3
import uuid

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

    # Determine HTTP method
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
    # 🔥 USER IS ALREADY VERIFIED BY API GATEWAY JWT AUTHORIZER
    # -----------------------------------------------------
    auth = event.get("requestContext", {}).get("authorizer", {})

    # Extract claims
    claims = auth.get("jwt", {}).get("claims", {}) or auth.get("claims", {})

    print("JWT Claims:", claims)

    # Since API Gateway validated JWT, this will always exist
    user_id = claims.get("email") or claims.get("cognito:username")
    
    if not user_id:
        return {
            "statusCode": 401,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Unauthorized: Please log in"})
        }

    # -----------------------------------------------------

    # Parse request body
    body_str = event.get("body") or "{}"
    try:
        body = json.loads(body_str)
    except:
        body = {}

    # Insert into DynamoDB
    table.put_item(
        Item={
            "user_id": user_id,
            "item_id": str(uuid.uuid4()),
            "product_id": body.get("product_id"),
            "name": body.get("name"),
            "price": body.get("price"),
            "image": body.get("image"),
            "qty": 1
        }
    )

    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps({"message": "Added to cart"})
    }
