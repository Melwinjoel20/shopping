import json
import boto3
import uuid
import os
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Attr

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")

orders_table = dynamodb.Table("Orders")
cart_table = dynamodb.Table("UserCart")


def clean_decimal(obj):
    if isinstance(obj, list):
        return [clean_decimal(i) for i in obj]
    if isinstance(obj, dict):
        return {k: clean_decimal(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, list):
        return [to_decimal(i) for i in obj]
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
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

    # -----------------------------------------------------
    # Detect HTTP method
    # -----------------------------------------------------
    method = (
        (event.get("requestContext", {}) or {})
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

    # -----------------------------------------------------
    # Parse request body
    # -----------------------------------------------------
    body_str = event.get("body") or "{}"
    try:
        body = json.loads(body_str)
    except Exception:
        body = {}

    user_id = body.get("user_id")

    if not user_id:
        return {
            "statusCode": 400,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "user_id is required"})
        }

    customer = body.get("customer") or {}
    payment_method = body.get("payment_method", "card")

    # -----------------------------------------------------
    # Load cart items for this user
    # -----------------------------------------------------
    cart_response = cart_table.scan(
        FilterExpression=Attr("user_id").eq(user_id)
    )

    items = clean_decimal(cart_response.get("Items", []))

    if not items:
        return {
            "statusCode": 400,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Cart is empty"})
        }

    # -----------------------------------------------------
    # Normalize cart items (IMPORTANT for analytics)
    # -----------------------------------------------------
    normalized_items = []

    for item in items:
        normalized_items.append({
            "item_id": item.get("item_id"),
            "product_id": item.get("product_id"),
            "name": item.get("name", "Unknown"),
            "price": float(item.get("price", 0)),
            "qty": int(item.get("qty", 1)),
            "image": item.get("image", ""),
            "category": item.get("category", "General"),   # ✅ IMPORTANT
            "user_id": item.get("user_id")
        })

    # -----------------------------------------------------
    # Calculate total
    # -----------------------------------------------------
    total = sum(i["price"] * i["qty"] for i in normalized_items)

    # -----------------------------------------------------
    # Create order
    # -----------------------------------------------------
    order_id = str(uuid.uuid4())

    orders_table.put_item(
        Item={
            "order_id": order_id,
            "user_id": user_id,
            "items": to_decimal(normalized_items),
            "total_amount": Decimal(str(total)),
            "payment_method": payment_method,
            "customer_details": to_decimal(customer),
            "status": "Pending",
            "created_at": datetime.utcnow().isoformat()
        }
    )

    # -----------------------------------------------------
    # Clear cart after successful order
    # -----------------------------------------------------
    for item in items:
        cart_table.delete_item(
            Key={
                "user_id": item["user_id"],
                "item_id": item["item_id"]
            }
        )

    # -----------------------------------------------------
    # SNS email notification
    # -----------------------------------------------------
    customer_email = customer.get("email")

    if SNS_TOPIC_ARN and customer_email:
        try:
            subs = sns.list_subscriptions_by_topic(TopicArn=SNS_TOPIC_ARN)
            existing = any(
                s.get("Endpoint") == customer_email
                for s in subs.get("Subscriptions", [])
            )

            if not existing:
                sns.subscribe(
                    TopicArn=SNS_TOPIC_ARN,
                    Protocol="email",
                    Endpoint=customer_email
                )
            else:
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject="EasyCart Order Confirmation",
                    Message=(
                        f"Your EasyCart order was placed successfully!\n\n"
                        f"Order ID: {order_id}\n"
                        f"Total Amount: €{total:.2f}\n"
                        f"Customer: {customer.get('full_name')}\n\n"
                        f"Thank you for shopping with EasyCart!"
                    )
                )
        except Exception as e:
            print("SNS Error:", e)

    # -----------------------------------------------------
    # Success response
    # -----------------------------------------------------
    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps({
            "message": "Order placed successfully",
            "order_id": order_id
        })
    }