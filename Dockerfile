# Multi-stage secure Dockerfile for TransactionFlow
FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# Final runtime stage
FROM python:3.11-slim AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install runtime utilities for container healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Create non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup -d /app -s /sbin/nologin appuser

# Copy application source code
COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY mock_api/ /app/mock_api/
COPY sql/ /app/sql/
COPY tests/ /app/tests/
COPY pyproject.toml /app/
COPY README.md /app/

# Ensure data directory exists with correct permissions
RUN mkdir -p /app/data && chown -R appuser:appgroup /app

USER appuser

# Default command starts mock API or can be overridden for ETL runs
CMD ["uvicorn", "mock_api.app:app", "--host", "0.0.0.0", "--port", "8000"]
