import pytest
from redis import Redis
from autocomplete.cache import SuggestionCache


@pytest.fixture
def redis_client():
    r = Redis(host="localhost", port=6379, db=15, decode_responses=True)
    yield r
    r.flushdb()


def test_cache_get_set(redis_client):
    cache = SuggestionCache(redis_client, key_prefix="test:cache", ttl_seconds=60)
    assert cache.get("wea") is None
    cache.set("wea", [{"text": "weather", "score": 0.9}])
    assert cache.get("wea") == [{"text": "weather", "score": 0.9}]
