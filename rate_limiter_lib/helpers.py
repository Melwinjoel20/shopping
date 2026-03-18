from .dynamo_backend import DynamoBackend
from .limiter import RateLimiter
from .exceptions import RateLimitExceeded
from django.conf import settings

def check_rate_limit(key, limit=5, window=60):
    backend = DynamoBackend(
        table_name=settings.RATE_LIMIT_TABLE,
        region=settings.COGNITO["region"]
    )

    limiter = RateLimiter(backend, limit=limit, window=window)

    try:
        limiter.allow(key)
        return True
    except RateLimitExceeded:
        return False
