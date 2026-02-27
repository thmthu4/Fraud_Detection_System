# 🛡️ Real-Time Fraud Detection System

A complete real-time fraud detection pipeline for **digital wallet transactions** built with:

- **Apache Kafka** — Message streaming for real-time transaction ingestion
- **Apache Spark** — ML model training (RandomForest) + Structured Streaming for predictions
- **Redis** — In-memory feature store, fraud alerts, and real-time counters
- **MongoDB** — Persistent storage for transactions, predictions, and model metrics
- **Streamlit** — Real-time monitoring dashboard with dark-themed premium UI

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────┐     ┌──────────────────────┐
│   Dataset    │────▶│  Kafka   │────▶│  Spark Streaming     │
│  (CSV File)  │     │  Topic   │     │  + ML Prediction     │
└──────┬───────┘     └──────────┘     └──────┬───────┬───────┘
       │                                     │       │
       ▼                                     ▼       ▼
┌──────────────┐                      ┌──────┐  ┌────────┐
│ Spark ML     │                      │Redis │  │MongoDB │
│ Training     │                      │Cache │  │Storage │
└──────────────┘                      └───┬──┘  └────┬───┘
                                          │          │
                                          ▼          ▼
                                    ┌──────────────────┐
                                    │    Streamlit      │
                                    │    Dashboard      │
                                    └──────────────────┘
```

## 📋 Prerequisites

- **Docker & Docker Compose** — For running Kafka, Redis, and MongoDB
- **Python 3.9+** — For the application code
- **Java 11+** — Required by PySpark
- **Your dataset** — CSV file with transaction data

## 🚀 Quick Start

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Place Your Dataset

Put your CSV dataset in the `data/` folder:

```bash
cp /path/to/your/dataset.csv data/dataset.csv
```

**Expected columns** (standard fraud detection format):
| Column | Description |
|--------|-------------|
| `step` | Time step (1 step = 1 hour) |
| `type` | Transaction type (PAYMENT, TRANSFER, CASH_OUT, etc.) |
| `amount` | Transaction amount |
| `nameOrig` | Origin account name |
| `oldbalanceOrg` | Origin balance before |
| `newbalanceOrig` | Origin balance after |
| `nameDest` | Destination account name |
| `oldbalanceDest` | Destination balance before |
| `newbalanceDest` | Destination balance after |
| `isFraud` | Fraud label (0 or 1) |

### 3. Start Infrastructure Services

```bash
./scripts/start_services.sh
```

This starts Kafka, Redis, MongoDB, initializes indexes, and creates topics.

### 4. Run the Full Pipeline

```bash
./scripts/run_pipeline.sh data/dataset.csv
```

This will:
1. 🧠 **Train the ML model** using your dataset
2. 📡 **Start Spark Streaming** consumer
3. 📤 **Start Kafka Producer** (streams your dataset)
4. 🌐 **Launch Dashboard** at http://localhost:8501

## 📦 Running Components Individually

### Train Model Only

```bash
python model_training/train_model.py data/dataset.csv
```

### Evaluate Model

```bash
python model_training/evaluate_model.py data/dataset.csv
```

### Start Stream Processor

```bash
python spark_streaming/stream_processor.py
```

### Start Kafka Producer

```bash
python kafka_producer/producer.py --dataset data/dataset.csv --delay 100 --limit 5000
```

### Launch Dashboard

```bash
streamlit run dashboard/app.py --server.port 8501
```

## 🗂️ Project Structure

```
├── docker-compose.yml              # Kafka, Zookeeper, Redis, MongoDB
├── requirements.txt                # Python dependencies
├── .env                            # Configuration variables
├── config/
│   └── settings.py                 # Centralized configuration
├── model_training/
│   ├── train_model.py              # Spark ML pipeline
│   └── evaluate_model.py           # Model evaluation report
├── kafka_producer/
│   └── producer.py                 # Transaction simulator → Kafka
├── spark_streaming/
│   └── stream_processor.py         # Kafka consumer + fraud prediction
├── feature_store/
│   └── redis_client.py             # Redis feature store
├── database/
│   ├── mongo_client.py             # MongoDB operations
│   └── init_db.py                  # Initialize MongoDB indexes
├── dashboard/
│   └── app.py                      # Streamlit real-time dashboard
├── scripts/
│   ├── start_services.sh           # Start Docker services
│   ├── run_pipeline.sh             # Run full pipeline
│   └── stop_services.sh            # Stop all services
├── data/                           # Your dataset goes here
└── models/                         # Trained models saved here
```

## ⚙️ Configuration

All settings are in `.env`. Key parameters:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `wallet_transactions` | Topic name |
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection |
| `REDIS_HOST` | `localhost` | Redis host |
| `PRODUCER_DELAY_MS` | `100` | Delay between messages (ms) |
| `SPARK_MASTER` | `local[*]` | Spark master URL |

## 🔧 Cleanup

```bash
./scripts/stop_services.sh
```

## 📊 Dashboard Features

- **KPI Cards** — Total transactions, fraud detected, fraud rate, avg amount
- **Transaction Feed** — Live table with fraud probability color-coding
- **Fraud Alerts** — Real-time red-highlighted alerts for high-risk transactions
- **Type Analysis** — Bar chart of fraud by transaction type
- **Distribution** — Donut chart of fraud vs legitimate
- **Timeline** — Time-series of transaction volume and fraud rate
- **Model Metrics** — AUC-ROC, accuracy, precision, recall, F1
- **Auto-Refresh** — Updates every 5 seconds
