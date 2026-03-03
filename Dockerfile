FROM python:3.12-slim

# Install Java for PySpark (multi-arch compatible)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Auto-detect JAVA_HOME for any architecture (amd64 or arm64)
ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY config/ config/
COPY model_training/ model_training/
COPY kafka_producer/ kafka_producer/
COPY spark_streaming/ spark_streaming/
COPY feature_store/ feature_store/
COPY database/ database/
COPY notifications/ notifications/
COPY dashboard/ dashboard/
COPY dashboard/pages/ dashboard/pages/
COPY data/ data/
COPY scripts/ scripts/

RUN chmod +x scripts/*.sh
