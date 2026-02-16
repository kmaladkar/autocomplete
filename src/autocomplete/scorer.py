"""Load model and score candidate suggestions."""

from pathlib import Path
from typing import Any

import joblib
import pandas as pd
import yaml


def load_model(model_path: str | Path, metadata_path: str | Path | None = None) -> tuple[Any, list[str]]:
    """Load joblib model and feature column list."""
    model_path = Path(model_path)
    meta_path = Path(metadata_path or model_path.parent / "metadata.yaml")
    model = joblib.load(model_path)
    feature_cols = []
    if meta_path.exists():
        with open(meta_path) as f:
            meta = yaml.safe_load(f) or {}
        feature_cols = meta.get("feature_columns", [])
    return model, feature_cols


def score_candidates(
    model: Any,
    feature_columns: list[str],
    prefix: str,
    candidates: list[str],
    query_stats: dict[str, dict[str, float]] | None = None,
) -> list[tuple[str, float]]:
    """Score candidate suggestions for a prefix. query_stats: suggestion -> {query_count, mean_position, ...}."""
    if not candidates:
        return []
    query_stats = query_stats or {}
    rows = []
    for sug in candidates:
        row = {
            "query_len": len(prefix),
            "suggestion_len": len(sug),
            "prefix_match_len": sum(1 for a, b in zip(prefix, sug) if a == b),
            "position": 0,
            "clicked_query_count": query_stats.get(sug, {}).get("query_count", 0),
            "clicked_sum_position": query_stats.get(sug, {}).get("sum_position", 0),
            "clicked_mean_position": query_stats.get(sug, {}).get("mean_position", 0),
            "clicked_ctr_approx": query_stats.get(sug, {}).get("ctr_approx", 0),
            "prefix_query_count": query_stats.get(prefix, {}).get("query_count", 0),
            "prefix_sum_position": query_stats.get(prefix, {}).get("sum_position", 0),
            "prefix_mean_position": query_stats.get(prefix, {}).get("mean_position", 0),
            "prefix_ctr_approx": query_stats.get(prefix, {}).get("ctr_approx", 0),
        }
        for c in feature_columns:
            if c not in row:
                row[c] = 0
        rows.append(row)
    X = pd.DataFrame(rows)[feature_columns] if feature_columns else pd.DataFrame(rows)
    X = X.reindex(columns=feature_columns, fill_value=0)
    scores = model.predict_proba(X)[:, 1] if hasattr(model, "predict_proba") else model.predict(X)
    return list(zip(candidates, scores.tolist() if hasattr(scores, "tolist") else [float(scores)]))
