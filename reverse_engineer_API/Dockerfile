FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for matplotlib and other libraries
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create directories for logs and metrics
RUN mkdir -p /app/logs /app/metrics

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the scheduler
CMD ["python", "scheduler.py"] 