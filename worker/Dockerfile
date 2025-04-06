FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install watchdog


COPY ./app .

# Use this instead of regular CMD
CMD ["watchmedo", "auto-restart", \
     "--directory=./app", \
     "--pattern=*.py", \
     "--recursive", \
     "--", \
     "python", "app/processor.py"]