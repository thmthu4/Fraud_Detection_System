"""
Notification Service — Simulated email notifications for fraud alerts.

Generates email-like notifications and stores them in MongoDB.
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import settings


class NotificationService:
    """Simulated email notification service for fraud alerts."""

    def __init__(self, mongo_client=None):
        self.mongo = mongo_client
        if self.mongo:
            self.collection = self.mongo.db["notifications"]

    def send_fraud_alert(self, tx_data):
        """Generate and store a fraud alert notification."""
        username = tx_data.get("username", "Customer")
        email = tx_data.get("email", "unknown@example.com")
        amount = tx_data.get("amount_src", 0)
        channel = tx_data.get("channel", "unknown")
        prob = tx_data.get("fraud_probability", 0)
        tx_id = tx_data.get("transaction_id", "N/A")
        action = tx_data.get("action", "flagged")

        subject = "Suspicious Transaction Alert — Action Required"

        body = (
            f"Dear {username},\n\n"
            f"We detected a suspicious transaction on your digital wallet account.\n\n"
            f"Transaction Details:\n"
            f"  Transaction ID: {tx_id}\n"
            f"  Amount: ${amount:,.2f}\n"
            f"  Channel: {channel}\n"
            f"  Risk Score: {prob:.3f}\n"
            f"  Action Taken: {action.upper()}\n\n"
            f"If you did not authorize this transaction, please contact support immediately.\n"
            f"If this was you, you can ignore this message.\n\n"
            f"— Fraud Detection Team"
        )

        notification = {
            "to": email,
            "username": username,
            "subject": subject,
            "body": body,
            "transaction_id": tx_id,
            "amount": amount,
            "channel": channel,
            "fraud_probability": prob,
            "action": action,
            "status": "sent",
            "created_at": datetime.utcnow().isoformat(),
        }

        if self.mongo and self.collection is not None:
            self.collection.insert_one(notification)

        return notification

    def send_block_notice(self, tx_data):
        """Generate and store a transaction block notification."""
        username = tx_data.get("username", "Customer")
        email = tx_data.get("email", "unknown@example.com")
        amount = tx_data.get("amount_src", 0)
        tx_id = tx_data.get("transaction_id", "N/A")

        subject = "Transaction Blocked — Fraud Prevention"

        body = (
            f"Dear {username},\n\n"
            f"A transaction of ${amount:,.2f} on your account has been BLOCKED\n"
            f"due to high fraud risk.\n\n"
            f"Transaction ID: {tx_id}\n\n"
            f"If this was a legitimate transaction, please contact support\n"
            f"to verify your identity and unblock it.\n\n"
            f"— Fraud Detection Team"
        )

        notification = {
            "to": email,
            "username": username,
            "subject": subject,
            "body": body,
            "transaction_id": tx_id,
            "amount": amount,
            "action": "blocked",
            "status": "sent",
            "created_at": datetime.utcnow().isoformat(),
        }

        if self.mongo and self.collection is not None:
            self.collection.insert_one(notification)

        return notification

    def get_recent_notifications(self, limit=20):
        """Get recent notifications."""
        if self.mongo and self.collection is not None:
            return list(
                self.collection.find(
                    {}, {"_id": 0}
                ).sort("created_at", -1).limit(limit)
            )
        return []

    def get_notification_stats(self):
        """Get notification statistics."""
        if self.mongo and self.collection is not None:
            total = self.collection.count_documents({})
            blocked = self.collection.count_documents({"action": "blocked"})
            flagged = self.collection.count_documents({"action": "flagged"})
            return {"total": total, "blocked": blocked, "flagged": flagged}
        return {"total": 0, "blocked": 0, "flagged": 0}
