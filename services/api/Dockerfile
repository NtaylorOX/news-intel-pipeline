FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml uv.lock* ./
RUN pip install --no-cache-dir -U pip setuptools && \
    pip install --no-cache-dir uv && \
    uv pip install --system -r pyproject.toml

# Copy application code
COPY app ./app
COPY services/api ./services/api

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Expose port
EXPOSE 8000

# Default command (can be overridden)
CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
