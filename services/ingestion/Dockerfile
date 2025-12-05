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
COPY services/ingestion ./services/ingestion

# No exposed ports for worker service
# EXPOSE is omitted for background workers

# Start ingestion worker
CMD ["python", "services/ingestion/main.py"]
