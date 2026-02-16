from typing import Any


import pytest
from redis import Redis
from autocomplete.trie import RedisTrie


@pytest.fixture
def redis_client():
    r = Redis(host="localhost", port=6379, db=15, decode_responses=True)
    yield r
    r.flushdb()


def test_trie_insert_and_search(redis_client):
    trie = RedisTrie(redis_client, key_prefix="test:trie")
    trie.insert("weather")
    trie.insert("weather london")
    assert set[Any](trie.prefix_completions("wea")) == {"weather", "weather london"}
    assert set[Any](trie.prefix_completions("weather")) == {"weather", "weather london"}
    assert trie.prefix_completions("xyz") == []
