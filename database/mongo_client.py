"""
MongoDB Client — Persistent storage for transactions and model metrics.

Handles all read/write operations to MongoDB for the fraud detection system.
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure

from config import settings


class MongoDBClient:
    """MongoDB client for the fraud detection system."""

    def __init__(self):
        self.client = MongoClient(
            settings.MONGO_URI,
            serverSelectionTimeoutMS=5000,
        )
        self.db = self.client[settings.MONGO_DATABASE]
        self.transactions = self.db[settings.MONGO_COLLECTION_TRANSACTIONS]
        self.metrics = self.db[settings.MONGO_COLLECTION_METRICS]
        self._verify_connection()

    def _verify_connection(self):
        """Verify MongoDB connection."""
        try:
            self.client.admin.command("ping")
            print(f"✅ Connected to MongoDB at {settings.MONGO_URI}")
        except ConnectionFailure:
            print(f"⚠️  Could not connect to MongoDB at {settings.MONGO_URI}")
            raise

    # ─────────────────────────────────────────────
    # Transactions
    # ─────────────────────────────────────────────
    def insert_transaction(self, transaction_data):
        """Insert a single transaction with prediction results."""
        transaction_data["inserted_at"] = datetime.utcnow()
        result = self.transactions.insert_one(transaction_data)
        return result.inserted_id

    def insert_transactions_batch(self, transactions):
        """Insert multiple transactions at once."""
        for tx in transactions:
            tx["inserted_at"] = datetime.utcnow()
        if transactions:
            result = self.transactions.insert_many(transactions)
            return result.inserted_ids
        return []

    def get_transactions(self, filters=None, limit=100, sort_by="inserted_at"):
        """Query transactions with optional filters."""
        if filters is None:
            filters = {}
        cursor = (
            self.transactions.find(filters)
            .sort(sort_by, DESCENDING)
            .limit(limit)
        )
        return list(cursor)

    def get_fraud_transactions(self, limit=100):
        """Get transactions flagged as fraud."""
        return self.get_transactions(
            filters={"prediction": 1.0},
            limit=limit,
        )

    def get_recent_transactions(self, limit=50):
        """Get the most recent transactions."""
        return self.get_transactions(limit=limit)

    # ─────────────────────────────────────────────
    # Fraud Statistics (Aggregation Pipelines)
    # ─────────────────────────────────────────────
    def get_fraud_stats(self):
        """Get aggregated fraud statistics."""
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "total_transactions": {"$sum": 1},
                    "total_fraud": {
                        "$sum": {"$cond": [{"$eq": ["$prediction", 1.0]}, 1, 0]}
                    },
                    "total_amount": {"$sum": "$amount_src"},
                    "fraud_amount": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$prediction", 1.0]},
                                "$amount_src",
                                0,
                            ]
                        }
                    },
                    "avg_amount": {"$avg": "$amount_src"},
                    "avg_fraud_probability": {"$avg": "$fraud_probability"},
                }
            }
        ]
        result = list(self.transactions.aggregate(pipeline))
        if result:
            stats = result[0]
            stats.pop("_id", None)
            total = stats.get("total_transactions", 0)
            fraud = stats.get("total_fraud", 0)
            stats["fraud_rate"] = (fraud / total * 100) if total > 0 else 0.0
            return stats
        return {
            "total_transactions": 0,
            "total_fraud": 0,
            "total_amount": 0,
            "fraud_amount": 0,
            "avg_amount": 0,
            "avg_fraud_probability": 0,
            "fraud_rate": 0.0,
        }

    def get_fraud_by_channel(self):
        """Get fraud counts grouped by channel."""
        pipeline = [
            {
                "$group": {
                    "_id": "$channel",
                    "total": {"$sum": 1},
                    "fraud_count": {
                        "$sum": {"$cond": [{"$eq": ["$prediction", 1.0]}, 1, 0]}
                    },
                }
            },
            {"$sort": {"fraud_count": DESCENDING}},
        ]
        return list(self.transactions.aggregate(pipeline))

    def get_transactions_over_time(self, interval_minutes=5):
        """Get transaction counts over time intervals."""
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "$dateTrunc": {
                            "date": "$inserted_at",
                            "unit": "minute",
                            "binSize": interval_minutes,
                        }
                    },
                    "total": {"$sum": 1},
                    "fraud_count": {
                        "$sum": {"$cond": [{"$eq": ["$prediction", 1.0]}, 1, 0]}
                    },
                    "avg_amount": {"$avg": "$amount_src"},
                }
            },
            {"$sort": {"_id": ASCENDING}},
        ]
        return list(self.transactions.aggregate(pipeline))

    def get_fraud_probability_distribution(self):
        """Get distribution of fraud probabilities."""
        pipeline = [
            {
                "$bucket": {
                    "groupBy": "$fraud_probability",
                    "boundaries": [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01],
                    "default": "other",
                    "output": {"count": {"$sum": 1}},
                }
            }
        ]
        return list(self.transactions.aggregate(pipeline))

    # ─────────────────────────────────────────────
    # Model Metrics
    # ─────────────────────────────────────────────
    def save_model_metrics(self, metrics_data):
        """Save model evaluation metrics."""
        metrics_data["saved_at"] = datetime.utcnow()
        metrics_data["version"] = self._get_next_model_version()
        result = self.metrics.insert_one(metrics_data)
        return result.inserted_id

    def get_latest_metrics(self):
        """Get the most recent model metrics."""
        result = self.metrics.find_one(sort=[("saved_at", DESCENDING)])
        return result

    def _get_next_model_version(self):
        """Get the next model version number."""
        latest = self.metrics.find_one(sort=[("version", DESCENDING)])
        return (latest.get("version", 0) + 1) if latest else 1

    # ─────────────────────────────────────────────
    # Case Management
    # ─────────────────────────────────────────────
    def get_fraud_cases(self, status_filter=None, limit=50):
        """Get fraud cases for review."""
        query = {"prediction": 1.0}
        if status_filter and status_filter != "all":
            query["case_status"] = status_filter
        return list(
            self.transactions.find(query, {"_id": 0})
            .sort("inserted_at", DESCENDING)
            .limit(limit)
        )

    def update_case_status(self, transaction_id, new_status, reviewer="analyst"):
        """Update case status: confirmed, false_positive, under_review, pending."""
        result = self.transactions.update_one(
            {"transaction_id": transaction_id},
            {"$set": {
                "case_status": new_status,
                "reviewed_by": reviewer,
                "reviewed_at": datetime.utcnow().isoformat(),
            }},
        )
        return result.modified_count > 0

    def get_case_stats(self):
        """Get case management statistics."""
        pipeline = [
            {"$match": {"prediction": 1.0}},
            {"$group": {
                "_id": {"$ifNull": ["$case_status", "pending"]},
                "count": {"$sum": 1},
            }},
        ]
        results = list(self.transactions.aggregate(pipeline))
        stats = {"pending": 0, "confirmed": 0, "false_positive": 0, "under_review": 0}
        for r in results:
            stats[r["_id"]] = r["count"]
        return stats

    # ─────────────────────────────────────────────
    # Utilities
    # ─────────────────────────────────────────────
    def get_collection_stats(self):
        """Get collection statistics."""
        return {
            "transactions_count": self.transactions.count_documents({}),
            "metrics_count": self.metrics.count_documents({}),
        }

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()

