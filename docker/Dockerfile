FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/data /app/scripts

COPY ./scripts ./scripts

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
      requests pandas python-dotenv psycopg2-binary tzdata


RUN useradd -ms /bin/bash appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "scripts/extract_weather.py"]
