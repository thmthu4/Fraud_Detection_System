FROM python:3.12-slim

# Install Java for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

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
COPY dashboard/ dashboard/
COPY data/ data/
COPY scripts/ scripts/

RUN chmod +x scripts/*.sh
