FROM python:3.11-slim AS builder
WORKDIR /app
ENV PIP_NO_CACHE_DIR=1
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir build && pip wheel --no-cache-dir --no-deps -w /wheels .

FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/* && rm -rf /wheels
COPY src ./src
COPY configs ./configs
COPY models ./models
RUN mkdir -p /app/data/processed

ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
ENV MODEL_PATH=/app/models/model.joblib
ENV METADATA_PATH=/app/models/metadata.yaml
ENV QUERY_STATS_PATH=/app/data/processed/query_stats.parquet
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "autocomplete.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
