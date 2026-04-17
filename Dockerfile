FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "fastapi>=0.115.0" \
    "uvicorn[standard]>=0.32.0" \
    "httpx>=0.27.0" \
    "psycopg[binary]>=3.2.0" \
    "clickhouse-connect>=0.8.0" \
    "pydantic-settings>=2.6.0" \
    "confluent-kafka>=2.3.0"

COPY clickhouse/init ./clickhouse/init
COPY src ./src

EXPOSE 8080

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
