# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (psycopg2-binary/shapely wheels usually ok on slim)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Windows에서 UTF-16으로 저장된 requirements.txt도 pip이 읽을 수 있게 UTF-8로 정규화
COPY docker/normalize_requirements.py /tmp/normalize_requirements.py
COPY requirements.txt /tmp/requirements.txt
RUN python3 /tmp/normalize_requirements.py \
 && pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

