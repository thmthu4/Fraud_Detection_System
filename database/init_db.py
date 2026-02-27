"""
Initialize MongoDB — Create indexes for optimal query performance.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pymongo import ASCENDING, DESCENDING
from database.mongo_client import MongoDBClient


def initialize_database():
    """Create indexes and initialize collections."""
    print("🔧 Initializing MongoDB...")

    mongo = MongoDBClient()

    # ── Transaction Indexes ──
    print("📋 Creating transaction indexes...")

    mongo.transactions.create_index(
        [("inserted_at", DESCENDING)],
        name="idx_inserted_at",
    )
    mongo.transactions.create_index(
        [("prediction", ASCENDING)],
        name="idx_prediction",
    )
    mongo.transactions.create_index(
        [("transaction_id", ASCENDING)],
        name="idx_transaction_id",
        unique=True,
        sparse=True,
    )
    mongo.transactions.create_index(
        [("channel", ASCENDING), ("prediction", ASCENDING)],
        name="idx_channel_prediction",
    )
    mongo.transactions.create_index(
        [("customer_id", ASCENDING), ("inserted_at", DESCENDING)],
        name="idx_customer_time",
    )
    mongo.transactions.create_index(
        [("fraud_probability", DESCENDING)],
        name="idx_fraud_probability",
    )

    # ── Metrics Indexes ──
    print("📋 Creating metrics indexes...")
    mongo.metrics.create_index(
        [("saved_at", DESCENDING)],
        name="idx_metrics_saved_at",
    )
    mongo.metrics.create_index(
        [("version", DESCENDING)],
        name="idx_metrics_version",
    )

    print("✅ MongoDB initialization complete!")
    stats = mongo.get_collection_stats()
    print(f"   Transactions: {stats['transactions_count']} documents")
    print(f"   Metrics:      {stats['metrics_count']} documents")

    mongo.close()


if __name__ == "__main__":
    initialize_database()
