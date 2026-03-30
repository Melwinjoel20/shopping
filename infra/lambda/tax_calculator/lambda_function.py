import json

TAX_RATES = {
    "clothing": {
        "IE": 0,       # Zero rated in Ireland
        "GB": 0,       # Zero rated in UK
        "US": 0,       # No federal VAT
        "DE": 19,
        "FR": 20,
        "IT": 22,
        "ES": 21,
        "AU": 10,
        "CA": 5,
        "DEFAULT": 20,
    },
    "electronics": {
        "IE": 23,
        "GB": 20,
        "US": 0,
        "DE": 19,
        "FR": 20,
        "IT": 22,
        "ES": 21,
        "AU": 10,
        "CA": 5,
        "DEFAULT": 20,
    },
    "food": {
        "IE": 0,
        "GB": 0,
        "US": 0,
        "DE": 7,
        "FR": 5.5,
        "IT": 4,
        "ES": 4,
        "AU": 0,
        "CA": 0,
        "DEFAULT": 10,
    },
    "general": {
        "IE": 23,
        "GB": 20,
        "US": 0,
        "DE": 19,
        "FR": 20,
        "IT": 22,
        "ES": 21,
        "AU": 10,
        "CA": 5,
        "DEFAULT": 20,
    }
}

CURRENCY_MAP = {
    "IE": "EUR", "DE": "EUR", "FR": "EUR",
    "IT": "EUR", "ES": "EUR",
    "GB": "GBP",
    "US": "USD",
    "AU": "AUD",
    "CA": "CAD",
    "DEFAULT": "EUR"
}

def lambda_handler(event, context):
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type"
    }

    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": ""}

    try:
        body = json.loads(event.get("body", "{}"))

        price = body.get("price")
        country_code = body.get("country_code", "IE").upper().strip()
        category = body.get("category", "general").lower().strip()

        if price is None:
            return {
                "statusCode": 400,
                "headers": headers,
                "body": json.dumps({"error": "price is required"})
            }

        price = float(price)
        if price < 0:
            raise ValueError("price cannot be negative")

        if category not in TAX_RATES:
            category = "general"

        category_rates = TAX_RATES[category]
        tax_rate = category_rates.get(country_code, category_rates["DEFAULT"])
        currency = CURRENCY_MAP.get(country_code, CURRENCY_MAP["DEFAULT"])

        tax_amount = round(price * tax_rate / 100, 2)
        final_price = round(price + tax_amount, 2)

        return {
            "statusCode": 200,
            "headers": headers,
            "body": json.dumps({
                "original_price": round(price, 2),
                "country_code": country_code,
                "category": category,
                "tax_rate": tax_rate,
                "tax_amount": tax_amount,
                "final_price": final_price,
                "currency": currency
            })
        }

    except (ValueError, TypeError) as e:
        return {
            "statusCode": 400,
            "headers": headers,
            "body": json.dumps({"error": str(e)})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": headers,
            "body": json.dumps({"error": "Internal server error"})
        }