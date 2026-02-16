"""Redis hot/cold cache for suggestion results."""

import json
from typing import Any

import redis


class SuggestionCache:
    """Cache suggestion results: key = cache:prefix:<normalized_prefix>, value = JSON list of suggestions."""

    def __init__(
        self,
        client: redis.Redis,
        key_prefix: str = "autocomplete:cache",
        ttl_seconds: int = 3600,
    ) -> None:
        self.client = client
        self.prefix = key_prefix.rstrip(":")
        self.ttl = ttl_seconds

    def _key(self, prefix: str) -> str:
        return f"{self.prefix}:{prefix.strip().lower()}"

    def get(self, prefix: str) -> list[dict[str, Any]] | None:
        """Return cached suggestions or None if miss."""
        raw = self.client.get(self._key(prefix))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

    def set(self, prefix: str, suggestions: list[dict[str, Any]]) -> None:
        """Cache suggestions for the prefix."""
        key = self._key(prefix)
        self.client.setex(key, self.ttl, json.dumps(suggestions))

    def delete(self, prefix: str) -> None:
        """Invalidate cache for a prefix."""
        self.client.delete(self._key(prefix))
