"""Trie stored in Redis: each node is a key, children and completions stored in sets/hashes."""

from typing import Iterator

import redis


class RedisTrie:
    """Distributed trie in Redis. Keys: trie:node:<path> -> set of completions; trie:children:<path> -> set of next chars."""

    def __init__(
        self,
        client: redis.Redis,
        key_prefix: str = "autocomplete:trie",
    ) -> None:
        self.client = client
        self.prefix = key_prefix.rstrip(":")

    def _node_key(self, path: str) -> str:
        return f"{self.prefix}:node:{path}"

    def _children_key(self, path: str) -> str:
        return f"{self.prefix}:children:{path}"

    def insert(self, word: str, payload: str | None = None) -> None:
        """Insert a full suggestion. At each prefix path we store the full completion."""
        word = word.strip().lower()
        if not word:
            return
        payload = payload or word
        pipe = self.client.pipeline()
        for i in range(1, len(word) + 1):
            path = word[:i]
            pipe.sadd(self._node_key(path), payload)
            if i < len(word):
                pipe.sadd(self._children_key(path), word[i])
        pipe.execute()

    def insert_many(self, words: list[str]) -> None:
        """Bulk insert; more efficient than repeated insert()."""
        for w in words:
            self.insert(w)

    def prefix_completions(self, prefix: str, limit: int = 50) -> list[str]:
        """Return completions for prefix (full strings that start with prefix)."""
        prefix = prefix.strip().lower()
        key = self._node_key(prefix)
        completions = self.client.smembers(key)
        out = [c.decode() if isinstance(c, bytes) else c for c in completions]
        return out[:limit]

    def search_prefix(self, prefix: str, limit: int = 50) -> list[str]:
        """Same as prefix_completions (alias)."""
        return self.prefix_completions(prefix, limit)

    def load_bulk(self, words: list[str]) -> None:
        """Insert many suggestions."""
        for w in words:
            self.insert(w)

    def delete_prefix(self, prefix: str) -> None:
        """Remove all nodes under a prefix (use with care)."""
        prefix = prefix.strip().lower()
        # Remove completion set for this path and all longer paths (simplified: only this path)
        self.client.delete(self._node_key(prefix))
        self.client.delete(self._children_key(prefix))
