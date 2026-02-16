#!/usr/bin/env python3
"""Run the ML scorer manually: load model + trie candidates, score and print ranked suggestions."""
import argparse
import sys
from pathlib import Path

import pandas as pd
import redis
import yaml

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from autocomplete.scorer import load_model, score_candidates
from autocomplete.trie import RedisTrie


def main() -> None:
    parser = argparse.ArgumentParser(description="Score autocomplete candidates for a prefix using the trained model")
    parser.add_argument("prefix", nargs="?", default="wea", help="Query prefix (e.g. wea, pyt)")
    parser.add_argument("--limit", type=int, default=10, help="Max suggestions to return")
    parser.add_argument("--config", type=Path, default=root / "configs" / "api.yaml", help="Config YAML")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    redis_cfg = config.get("redis", {})
    host = redis_cfg.get("host", "localhost")
    port = int(redis_cfg.get("port", 6379))
    db = int(redis_cfg.get("db", 0))
    prefix_key = f"{redis_cfg.get('prefix', 'autocomplete')}:{redis_cfg.get('trie_key_prefix', 'trie')}"

    client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
    trie = RedisTrie(client, key_prefix=prefix_key)

    model_path = Path(config.get("model", {}).get("path", "models/model.joblib"))
    meta_path = Path(config.get("model", {}).get("metadata_path", "models/metadata.yaml"))
    stats_path = Path(config.get("query_stats_path", "data/processed/query_stats.parquet"))

    if not model_path.is_absolute():
        model_path = root / model_path
    if not meta_path.is_absolute():
        meta_path = root / meta_path
    if not stats_path.is_absolute():
        stats_path = root / stats_path

    prefix = args.prefix.strip().lower()
    candidates = trie.prefix_completions(prefix, limit=args.limit * 3)
    if not candidates:
        print(f"No candidates for prefix '{prefix}'. Is the trie loaded? (run scripts/run_load_trie.py)")
        return

    model, feature_cols = load_model(model_path, meta_path)
    query_stats = {}
    if stats_path.exists():
        df = pd.read_parquet(stats_path)
        query_stats = df.set_index("query").to_dict(orient="index")

    scored = score_candidates(model, feature_cols, prefix, candidates, query_stats)
    scored.sort(key=lambda x: x[1], reverse=True)
    top = scored[: args.limit]

    print(f"Prefix: '{prefix}' (showing top {len(top)} of {len(candidates)} candidates)\n")
    for i, (text, score) in enumerate(top, 1):
        print(f"  {i}. {text}  score={score:.4f}")
    print()


if __name__ == "__main__":
    main()
