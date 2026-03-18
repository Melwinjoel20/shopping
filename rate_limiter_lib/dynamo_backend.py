import time
import boto3
from decimal import Decimal

class DynamoBackend:
    def __init__(self, table_name, region="us-east-1"):
        self.table = boto3.resource("dynamodb", region_name=region).Table(table_name)

    def get(self, key):
        res = self.table.get_item(Key={"key": key})
        return res.get("Item")

    def create(self, key, window):
        now = int(time.time())
        self.table.put_item(
            Item={
                "key": key,
                "count": Decimal(1),
                "ttl": now + window
            }
        )

    def increment(self, key):
        self.table.update_item(
            Key={"key": key},
            UpdateExpression="SET #c = #c + :inc",
            ExpressionAttributeNames={"#c": "count"},
            ExpressionAttributeValues={":inc": Decimal(1)}
        )
