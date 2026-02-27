"""
Redis Feature Store Client.

Manages real-time features, fraud alerts, and per-user statistics
in Redis for the fraud detection pipeline.
"""

import sys
import json
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import redis

from config import settings


class RedisFeatureStore:
    """Redis client for real-time feature storage and fraud alerts."""

    def __init__(self):
        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
        )
        self._verify_connection()

    def _verify_connection(self):
        """Verify Redis connection."""
        try:
            self.client.ping()
            print(f"✅ Connected to Redis at {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        except redis.ConnectionError:
            print(f"⚠️  Could not connect to Redis at {settings.REDIS_HOST}:{settings.REDIS_PORT}")
            raise

    # ─────────────────────────────────────────────
    # Fraud Alerts
    # ─────────────────────────────────────────────
    def store_fraud_alert(self, transaction_id, transaction_data):
        """Store a flagged fraud transaction as an alert."""
        key = f"fraud_alert:{transaction_id}"
        data = {
            "transaction_id": transaction_id,
            "data": json.dumps(transaction_data),
            "timestamp": datetime.utcnow().isoformat(),
            "status": "active",
        }
        self.client.hset(key, mapping=data)
        self.client.expire(key, settings.REDIS_ALERT_TTL)

        # Add to sorted set for easy retrieval by time
        self.client.zadd(
            "fraud_alerts_timeline",
            {transaction_id: time.time()},
        )

    def get_recent_alerts(self, count=50):
        """Get the most recent fraud alerts."""
        # Get most recent alert IDs
        alert_ids = self.client.zrevrange("fraud_alerts_timeline", 0, count - 1)

        alerts = []
        for tid in alert_ids:
            key = f"fraud_alert:{tid}"
            data = self.client.hgetall(key)
            if data:
                if "data" in data:
                    data["data"] = json.loads(data["data"])
                alerts.append(data)

        return alerts

    def get_alert_count(self):
        """Get total number of active fraud alerts."""
        return self.client.zcard("fraud_alerts_timeline")

    # ─────────────────────────────────────────────
    # User Statistics (Rolling)
    # ─────────────────────────────────────────────
    def update_user_stats(self, user_id, amount, is_fraud=False):
        """Update per-user rolling statistics."""
        key = f"user_stats:{user_id}"

        pipe = self.client.pipeline()
        pipe.hincrby(key, "transaction_count", 1)
        pipe.hincrbyfloat(key, "total_amount", amount)
        if is_fraud:
            pipe.hincrby(key, "fraud_count", 1)
        pipe.expire(key, settings.REDIS_ALERT_TTL * 24)  # keep for 24h
        pipe.execute()

    def get_user_stats(self, user_id):
        """Get per-user statistics."""
        key = f"user_stats:{user_id}"
        stats = self.client.hgetall(key)
        if stats:
            return {
                "user_id": user_id,
                "transaction_count": int(stats.get("transaction_count", 0)),
                "total_amount": float(stats.get("total_amount", 0)),
                "fraud_count": int(stats.get("fraud_count", 0)),
                "avg_amount": (
                    float(stats.get("total_amount", 0))
                    / max(int(stats.get("transaction_count", 1)), 1)
                ),
            }
        return None

    # ─────────────────────────────────────────────
    # Real-Time Counters
    # ─────────────────────────────────────────────
    def increment_transaction_count(self):
        """Increment the global transaction counter."""
        return self.client.incr("total_transactions")

    def increment_fraud_count(self):
        """Increment the global fraud counter."""
        return self.client.incr("total_frauds")

    def get_global_stats(self):
        """Get global counters."""
        total_tx = self.client.get("total_transactions")
        total_fraud = self.client.get("total_frauds")
        return {
            "total_transactions": int(total_tx) if total_tx else 0,
            "total_frauds": int(total_fraud) if total_fraud else 0,
            "fraud_rate": (
                int(total_fraud) / max(int(total_tx), 1) * 100
                if total_tx and total_fraud
                else 0.0
            ),
        }

    # ─────────────────────────────────────────────
    # Recent Transactions (for dashboard feed)
    # ─────────────────────────────────────────────
    def store_recent_transaction(self, transaction_data):
        """Store a transaction in the recent feed (keeps last 200)."""
        self.client.lpush("recent_transactions", json.dumps(transaction_data))
        self.client.ltrim("recent_transactions", 0, 199)

    def get_recent_transactions(self, count=50):
        """Get most recent transactions from the feed."""
        items = self.client.lrange("recent_transactions", 0, count - 1)
        return [json.loads(item) for item in items]

    # ─────────────────────────────────────────────
    # Cleanup
    # ─────────────────────────────────────────────
    def flush_all(self):
        """Clear all data (use with caution)."""
        self.client.flushdb()
        print("🗑️  Redis data cleared")

    def close(self):
        """Close the Redis connection."""
        self.client.close()
