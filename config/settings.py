"""
Centralized configuration for the Fraud Detection System.
Loads settings from .env file with sensible defaults.
Configured for the Digital Wallet Transaction dataset.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")


# ─────────────────────────────────────────────
# Kafka
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wallet_transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud_detection_group")

# ─────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "fraud_detection")
MONGO_COLLECTION_TRANSACTIONS = os.getenv("MONGO_COLLECTION_TRANSACTIONS", "transactions")
MONGO_COLLECTION_METRICS = os.getenv("MONGO_COLLECTION_METRICS", "model_metrics")

# ─────────────────────────────────────────────
# Redis
# ─────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_ALERT_TTL = int(os.getenv("REDIS_ALERT_TTL", "3600"))

# ─────────────────────────────────────────────
# Spark
# ─────────────────────────────────────────────
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "FraudDetection")

# ─────────────────────────────────────────────
# Model
# ─────────────────────────────────────────────
MODEL_PATH = os.getenv("MODEL_PATH", str(PROJECT_ROOT / "models" / "fraud_model"))
DATASET_PATH = os.getenv("DATASET_PATH", str(PROJECT_ROOT / "data" / "dataset.csv"))

# ─────────────────────────────────────────────
# Producer
# ─────────────────────────────────────────────
PRODUCER_DELAY_MS = int(os.getenv("PRODUCER_DELAY_MS", "100"))

# ─────────────────────────────────────────────
# Dataset Schema — Digital Wallet Transactions
# ─────────────────────────────────────────────
# Numeric features used directly by the ML model
NUMERIC_FEATURE_COLUMNS = [
    "amount_src",
    "ip_risk_score",
    "account_age_days",
    "device_trust_score",
    "chargeback_history_count",
    "risk_score_internal",
    "txn_velocity_1h",
    "txn_velocity_24h",
    "corridor_risk",
    "new_device_flag",        # derived: TRUE/FALSE → 1/0
    "location_mismatch_flag", # derived: TRUE/FALSE → 1/0
    "hour_of_day",            # derived from timestamp
    "day_of_week",            # derived from timestamp
]

# Categorical features (will be StringIndexed)
CATEGORICAL_FEATURE_COLUMNS = [
    "channel",
    "ip_country",
    "kyc_tier",
]

# All feature columns after encoding (numeric + indexed categoricals)
FEATURE_COLUMNS = NUMERIC_FEATURE_COLUMNS + [
    "channel_index",
    "ip_country_index",
    "kyc_tier_index",
]

LABEL_COLUMN = "is_fraud"

CHANNEL_TYPES = ["atm", "web", "mobile"]
KYC_TIERS = ["standard", "enhanced", "basic", "premium"]
