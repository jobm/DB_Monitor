FROM python:3.11-slim

WORKDIR /app

# Install minimal system dependencies for asyncpg
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

# Copy dependencies first to leverage caching
COPY pyproject.toml uv.lock ./

# Install dependencies (system-wide since it's a docker container)
# Also prevent psycopg2 from being installed since asyncpg is used
RUN uv sync --frozen --no-install-project --no-dev && \
    uv pip uninstall --system -y psycopg2 psycopg2-binary 2>/dev/null || true

# Copy application code
COPY . .

# Install the project itself
RUN uv sync --frozen --no-dev

EXPOSE 8000
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
