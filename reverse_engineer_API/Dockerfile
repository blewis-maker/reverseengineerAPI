FROM python:3.9-slim

# Configure apt and install packages
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    cron \
    gdal-bin \
    libgdal-dev \
    gcc \
    g++ \
    python3-dev \
    krb5-user \
    libkrb5-dev \
    libssl-dev \
    git \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Configure krb5-config non-interactively
ENV DEBIAN_FRONTEND=noninteractive
RUN echo "krb5-config krb5-config/default_realm string EXAMPLE.COM" | debconf-set-selections && \
    echo "krb5-config krb5-config/kerberos_servers string localhost" | debconf-set-selections && \
    echo "krb5-config krb5-config/admin_server string localhost" | debconf-set-selections && \
    apt-get update && \
    apt-get install -y krb5-config && \
    rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables
ENV GDAL_VERSION=3.4.1
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt ./

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir wheel setuptools && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create log directory
RUN mkdir -p /app/logs

# Set timezone
RUN ln -snf /usr/share/zoneinfo/America/Denver /etc/localtime && echo America/Denver > /etc/timezone

# Setup cron job
COPY crontab /etc/cron.d/app-cron
RUN chmod 0644 /etc/cron.d/app-cron && \
    crontab /etc/cron.d/app-cron

# Create a startup script
RUN echo '#!/bin/sh\ntouch /app/logs/cron.log\nchmod 0666 /app/logs/cron.log\n/etc/init.d/cron start\ntail -f /app/logs/cron.log' > /start.sh && \
    chmod +x /start.sh

CMD ["/start.sh"]