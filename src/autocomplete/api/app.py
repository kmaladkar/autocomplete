"""FastAPI app: /suggest with Redis trie, cache, and ML ranking; Prometheus metrics."""

import os
import time
from pathlib import Path

import pandas as pd
import redis
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from autocomplete.cache import SuggestionCache
from autocomplete.trie import RedisTrie
from autocomplete.scorer import load_model, score_candidates

# --- Prometheus metrics ---
REQUEST_COUNT = Counter(
    "autocomplete_requests_total",
    "Total suggest requests",
    ["cache_hit"],
)
REQUEST_LATENCY = Histogram(
    "autocomplete_request_duration_seconds",
    "Suggest request latency",
    ["cache_hit"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0),
)
SUGGESTIONS_RETURNED = Histogram(
    "autocomplete_suggestions_returned",
    "Number of suggestions returned",
    buckets=(0, 1, 5, 10, 20, 50),
)

app = FastAPI(
    title="Autocomplete Search API",
    description="ML-ranked suggestions with Redis trie and cache",
    version="0.1.0",
)

_redis: redis.Redis | None = None
_trie: RedisTrie | None = None
_cache: SuggestionCache | None = None
_model = None
_model_features: list[str] = []
_query_stats: dict = {}


def _get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        db = int(os.getenv("REDIS_DB", "0"))
        _redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
    return _redis


def _get_trie() -> RedisTrie:
    global _trie
    if _trie is None:
        prefix = os.getenv("REDIS_TRIE_PREFIX", "autocomplete:trie")
        _trie = RedisTrie(_get_redis(), key_prefix=prefix)
    return _trie


def _get_cache() -> SuggestionCache:
    global _cache
    if _cache is None:
        prefix = os.getenv("REDIS_CACHE_PREFIX", "autocomplete:cache")
        ttl = int(os.getenv("CACHE_TTL_SECONDS", "3600"))
        _cache = SuggestionCache(_get_redis(), key_prefix=prefix, ttl_seconds=ttl)
    return _cache


def _get_model():
    global _model, _model_features, _query_stats
    if _model is None:
        model_path = Path(os.getenv("MODEL_PATH", "models/model.joblib"))
        meta_path = Path(os.getenv("METADATA_PATH", "models/metadata.yaml"))
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
        _model, _model_features = load_model(model_path, meta_path)
        stats_path = Path(os.getenv("QUERY_STATS_PATH", "data/processed/query_stats.parquet"))
        if stats_path.exists():
            df = pd.read_parquet(stats_path)
            _query_stats = df.set_index("query").to_dict(orient="index")
        else:
            _query_stats = {}
    return _model, _model_features, _query_stats


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health")
def health() -> dict:
    """Liveness: service is up."""
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict:
    """Readiness: Redis and model (if required) are available."""
    try:
        _get_redis().ping()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis: {e}")
    try:
        _get_model()
    except FileNotFoundError:
        pass  # model optional for readiness if we allow trie-only
    return {"status": "ready"}


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus scrape endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/suggest")
def suggest(
    q: str = Query(..., min_length=1, description="Search prefix"),
    limit: int = Query(10, ge=1, le=20),
) -> dict:
    """Return ML-ranked autocomplete suggestions. Uses Redis cache (hot) or trie + model (cold)."""
    prefix = q.strip().lower()
    cache = _get_cache()
    cached = cache.get(prefix)
    if cached is not None:
        REQUEST_COUNT.labels(cache_hit="true").inc()
        start = time.perf_counter()
        out = {"query": prefix, "suggestions": cached[:limit], "cached": True}
        REQUEST_LATENCY.labels(cache_hit="true").observe(time.perf_counter() - start)
        SUGGESTIONS_RETURNED.observe(len(out["suggestions"]))
        return out

    REQUEST_COUNT.labels(cache_hit="false").inc()
    start = time.perf_counter()
    trie = _get_trie()
    raw = trie.prefix_completions(prefix, limit=limit * 3)
    if not raw:
        out = {"query": prefix, "suggestions": [], "cached": False}
        cache.set(prefix, [])
        REQUEST_LATENCY.labels(cache_hit="false").observe(time.perf_counter() - start)
        SUGGESTIONS_RETURNED.observe(0)
        return out

    try:
        model, feature_cols, query_stats = _get_model()
        scored = score_candidates(model, feature_cols, prefix, raw, query_stats)
        scored.sort(key=lambda x: x[1], reverse=True)
        suggestions = [{"text": s[0], "score": round(s[1], 4)} for s in scored[:limit]]
    except FileNotFoundError:
        suggestions = [{"text": s, "score": 1.0} for s in raw[:limit]]

    cache.set(prefix, suggestions)
    REQUEST_LATENCY.labels(cache_hit="false").observe(time.perf_counter() - start)
    SUGGESTIONS_RETURNED.observe(len(suggestions))
    return {"query": prefix, "suggestions": suggestions, "cached": False}
