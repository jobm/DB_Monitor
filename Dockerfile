FROM python:3.11-slim

WORKDIR /app

# Install minimal system dependencies for asyncpg
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

# Copy project metadata first for layer caching
COPY pyproject.toml uv.lock README.md ./

# Install runtime dependencies
RUN uv sync --frozen --no-install-project --no-dev && \
    uv pip uninstall --system -y psycopg2 psycopg2-binary 2>/dev/null || true

# Copy application code and install the package itself
COPY src/ ./src/
COPY container-entrypoint.sh ./
RUN uv sync --frozen --no-dev

# Create non-root user
RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid 1001 --create-home --shell /usr/sbin/nologin appuser && \
    chown -R appuser:appuser /app

USER appuser

ENV UV_CACHE_DIR=/tmp/uv
ENV PYTHONPATH=/app/src:/app
# Ensure CLI entry points (db-monitor-serve, db-monitor-migrate) are on PATH
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

# Use the canonical CLI entrypoint.
# Supports DB_SCHEMA_MODE=apply|validate|skip via container-entrypoint.sh.
CMD ["sh", "/app/container-entrypoint.sh"]
