#!/usr/bin/env python3
"""Load vocabulary into Redis trie. Reads data/processed/vocabulary.parquet or a CSV list."""
import argparse
import sys
from pathlib import Path

import pandas as pd
import redis
import yaml

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from autocomplete.trie import RedisTrie


def load_config(config_path: Path) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load vocabulary into Redis trie")
    parser.add_argument(
        "--config",
        type=Path,
        default=root / "configs" / "api.yaml",
        help="Config YAML with redis and vocabulary_path",
    )
    args = parser.parse_args()
    config = load_config(args.config)

    redis_cfg = config.get("redis", {})
    host = redis_cfg.get("host", "localhost")
    port = int(redis_cfg.get("port", 6379))
    db = int(redis_cfg.get("db", 0))
    prefix = redis_cfg.get("prefix", "autocomplete")
    trie_key = redis_cfg.get("trie_key_prefix", "trie")
    key_prefix = f"{prefix}:{trie_key}"

    client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
    trie = RedisTrie(client, key_prefix=key_prefix)

    vocab_path = Path(config.get("vocabulary_path", "data/processed/vocabulary.parquet"))
    if not vocab_path.is_absolute():
        vocab_path = root / vocab_path
    if vocab_path.exists():
        df = pd.read_parquet(vocab_path)
        col = "query" if "query" in df.columns else df.columns[0]
        words = df[col].astype(str).str.strip().str.lower().drop_duplicates().tolist()
    else:
        # Fallback: sample vocabulary for demo
        words = [
            "weather", "weather london", "weather new york", "weather forecast",
            "python", "python tutorial", "python install",
            "github", "github actions", "github login",
            "redis", "redis cache", "redis docker",
        ]
        print("No vocabulary.parquet found; using sample words.")
    trie.load_bulk(words)
    print(f"Loaded {len(words)} suggestions into Redis trie.")


if __name__ == "__main__":
    main()
