# 🛡️ Real-Time Fraud Detection System

A real-time fraud detection pipeline for digital wallet transactions using **Kafka**, **Apache Spark**, **Redis**, **MongoDB**, and **Streamlit**.

## Architecture

```
CSV Dataset → Kafka Producer → Kafka Topic → Spark Streaming → ML Model (Logistic Regression)
                                                    ↓
                                              Redis (alerts) + MongoDB (storage)
                                                    ↓
                                            Streamlit Dashboard (localhost:8501)
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- No other dependencies needed — everything runs in containers

## Quick Start (Docker)

### 1. Clone and start

```bash
git clone https://github.com/thmthu4/Fraud_Detection_System.git
cd Fraud_Detection_System
docker-compose up --build -d
```

### 2. Open the dashboard

Go to **http://localhost:8501** in your browser.

### 3. Watch it work

The system will automatically:
1. Start infrastructure (Zookeeper → Kafka → Redis → MongoDB)
2. Train the Logistic Regression model on the dataset (~10 seconds)
3. Start the Spark Streaming consumer (subscribes to Kafka)
4. Stream transactions from `streaming_data.csv` into Kafka
5. Display real-time fraud detection results on the dashboard

### 4. Check logs

```bash
# See all service logs
docker-compose logs -f

# See specific service logs
docker-compose logs -f trainer        # Model training progress
docker-compose logs -f stream-processor  # Spark streaming output
docker-compose logs -f producer       # Transaction streaming
docker-compose logs -f dashboard      # Streamlit dashboard
```

### 5. Stop everything

```bash
docker-compose down           # Stop and remove containers
docker-compose down -v        # Also remove data volumes (clean reset)
```

### 6. Restart (no rebuild needed)

```bash
docker-compose up -d          # Start again (uses cached images)
docker-compose up --build -d  # Rebuild if code changed
```

## Local Development

If you want to run the Python code on your machine (not in Docker):

### Requirements
- Python 3.12
- Java 17 (for PySpark)

### Setup

```bash
# Start only infrastructure in Docker
docker-compose up -d zookeeper kafka redis mongodb

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Train the model
python3 model_training/train_model.py data/dataset.csv

# Run the full pipeline (streaming + dashboard)
./scripts/run_pipeline.sh data/dataset.csv

# Open http://localhost:8501
```

### Useful commands

```bash
# Evaluate model performance
python3 model_training/evaluate_model.py data/dataset.csv

# Force retrain the model
./scripts/run_pipeline.sh data/dataset.csv --retrain

# Run only the producer
python3 kafka_producer/producer.py --dataset data/streaming_data.csv --delay 50

# Run only the dashboard
streamlit run dashboard/app.py --server.port 8501
```

## Model Performance

| Metric | Value |
|--------|-------|
| Algorithm | Logistic Regression (Spark ML) |
| Training Time | ~10 seconds |
| AUC-ROC | 0.9427 |
| Accuracy | 97.85% |
| Fraud Precision | 99.32% |
| Fraud Recall | 77.78% |
| False Positives | 1 |
| Dataset | 10,385 transactions (9.5% fraud) |
| Features | 16 engineered features |

### Confusion Matrix

```
                Predicted
                Legit    Fraud
Actual Legit |   1814        1     ← Almost zero false alarms
Actual Fraud |     42      147     ← Catches 78% of fraud
```

## Project Structure

```
├── config/              # Centralized settings (env-based)
├── dashboard/           # Streamlit real-time dashboard
├── data/
│   ├── dataset.csv      # Training dataset (10,385 rows)
│   └── streaming_data.csv  # Streaming dataset (with Burmese names)
├── database/            # MongoDB client and init scripts
├── feature_store/       # Redis feature store
├── kafka_producer/      # Transaction stream simulator
├── model_training/
│   ├── train_model.py   # Spark ML training pipeline
│   └── evaluate_model.py  # Model evaluation with metrics
├── scripts/
│   ├── run_pipeline.sh  # Full pipeline runner
│   ├── start_services.sh  # Start Docker infrastructure
│   └── stop_services.sh   # Stop Docker infrastructure
├── spark_streaming/     # Spark Structured Streaming consumer
├── docker-compose.yml   # Full stack (8 services)
├── Dockerfile           # Python app container
└── requirements.txt     # Python dependencies
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Ingestion | Apache Kafka |
| Processing | Apache Spark (Structured Streaming) |
| ML Model | Spark ML — Logistic Regression |
| Feature Store | Redis |
| Database | MongoDB |
| Dashboard | Streamlit + Plotly |
| Container | Docker + Docker Compose |
