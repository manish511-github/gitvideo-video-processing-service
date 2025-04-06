FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./app .

# Create directories for mounted volumes
# RUN mkdir -p trimmed hls temp

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]