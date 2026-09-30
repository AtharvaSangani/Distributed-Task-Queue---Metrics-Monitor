FROM python:3.10-slim

# Prevent Python from buffering stdout/stderr and writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies (build-essential for gevent/locust if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

# Copy application source code
COPY . .

# Default port for Producer API
EXPOSE 8000

# Default command runs Producer API
CMD ["uvicorn", "src.producer:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
