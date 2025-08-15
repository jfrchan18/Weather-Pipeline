# 1) Small, stable base
FROM python:3.10-slim

# 2) Sensible defaults
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# 3) System deps (keep it tiny)
# - ca-certificates for HTTPS APIs
# - (optional) curl for quick health/debug
# We install tzdata via pip (see below) to avoid Debian tzdata prompts.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# 4) Create writable dirs for bind mounts
RUN mkdir -p /app/data /app/scripts

# 5) Copy only what we need for caching
# If you later add a requirements.txt, copy it before "pip install" for better caching.
COPY ./scripts ./scripts

# 6) Python deps
# - psycopg2-binary avoids system libpq build
# - tzdata (PyPI) gives ZoneInfo data on slim images (useful for Asia/Manila, pandas)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
      requests pandas python-dotenv psycopg2-binary tzdata

# 7) Run as non-root
RUN useradd -ms /bin/bash appuser && chown -R appuser:appuser /app
USER appuser

# 8) (Optional) simple healthcheck (doesn't fail container if DB is down)
# HEALTHCHECK CMD python - <<'PY' || exit 1
# import os, socket; s=socket.socket(); s.settimeout(2)
# s.connect((os.getenv("DB_HOST","postgres"), int(os.getenv("DB_PORT","5432")))); s.close()
# print("ok")
# PY

# 9) Default command (Compose can override)
CMD ["python", "scripts/extract_weather.py"]
