from .exceptions import RateLimitExceeded

class RateLimiter:
    def __init__(self, backend, limit=5, window=60):
        self.backend = backend
        self.limit = limit
        self.window = window

    def allow(self, key):
        record = self.backend.get(key)

        if not record:
            self.backend.create(key, self.window)
            return True

        count = int(record.get("count", 0))

        if count >= self.limit:
            raise RateLimitExceeded(f"Rate limit exceeded for key: {key}")

        self.backend.increment(key)
        return True
