FROM python:3.10-slim-bookworm

# Install system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    libjpeg-dev \
    libfreetype6-dev \
    libpng-dev \
    liblapack-dev \
    gfortran \
    curl \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt
COPY requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Expose debug port for debugpy
EXPOSE 5678

WORKDIR /app
