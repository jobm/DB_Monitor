FROM python:3.11-slim

WORKDIR /app

# Install minimal system dependencies for asyncpg
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install Python dependencies explicitly avoiding psycopg2
RUN pip install --no-cache-dir -r requirements.txt && \
    pip uninstall -y psycopg2 psycopg2-binary 2>/dev/null || true

COPY . .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
