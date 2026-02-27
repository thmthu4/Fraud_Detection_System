"""
Kafka Producer — Transaction Simulator.

Reads the CSV dataset row by row and publishes each transaction
as a JSON message to the Kafka topic, simulating a real-time
digital wallet transaction stream.
"""

import sys
import json
import time
import uuid
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import settings


def create_producer(retries=5, delay=3):
    """Create a Kafka producer with retry logic."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
            print(f"✅ Connected to Kafka at {settings.KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            if attempt < retries - 1:
                print(f"⚠️  Kafka not available, retrying in {delay}s... ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                raise Exception(
                    f"❌ Could not connect to Kafka after {retries} attempts. "
                    f"Is Kafka running at {settings.KAFKA_BOOTSTRAP_SERVERS}?"
                )


def stream_transactions(dataset_path=None, delay_ms=None, limit=None):
    """
    Read CSV dataset and stream each row as a JSON message to Kafka.

    Args:
        dataset_path: Path to the CSV dataset.
        delay_ms: Delay between messages in milliseconds. 
        limit: Max number of transactions to send (None = all).
    """
    if dataset_path is None:
        dataset_path = settings.DATASET_PATH
    if delay_ms is None:
        delay_ms = settings.PRODUCER_DELAY_MS

    print(f"📂 Loading dataset from: {dataset_path}")
    df = pd.read_csv(dataset_path)
    print(f"✅ Loaded {len(df)} transactions")

    producer = create_producer()
    topic = settings.KAFKA_TOPIC

    sent = 0
    start_time = time.time()

    try:
        for idx, row in df.iterrows():
            if limit and sent >= limit:
                break

            # Build transaction message
            transaction = row.to_dict()

            # Add metadata
            transaction["transaction_id"] = str(uuid.uuid4())
            transaction["timestamp"] = datetime.utcnow().isoformat()
            transaction["ingestion_time"] = time.time()

            # Convert numpy types to Python types for JSON serialization
            for key, value in transaction.items():
                if hasattr(value, "item"):  # numpy scalar
                    transaction[key] = value.item()
                elif pd.isna(value):
                    transaction[key] = 0.0

            # Send to Kafka
            producer.send(
                topic,
                key=transaction.get("transaction_id"),
                value=transaction,
            )

            sent += 1

            if sent % 100 == 0:
                producer.flush()
                elapsed = time.time() - start_time
                rate = sent / elapsed if elapsed > 0 else 0
                print(f"📤 Sent {sent} transactions ({rate:.1f} tx/sec)")

            # Simulate real-time delay
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)

    except KeyboardInterrupt:
        print(f"\n⏹️  Stopped by user after {sent} transactions")
    finally:
        producer.flush()
        producer.close()

    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"✅ Streaming complete!")
    print(f"   Total sent: {sent} transactions")
    print(f"   Duration:   {elapsed:.2f}s")
    print(f"   Avg rate:   {sent/elapsed:.1f} tx/sec")
    print(f"{'='*50}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stream transactions to Kafka")
    parser.add_argument("--dataset", type=str, default=None, help="Path to CSV dataset")
    parser.add_argument("--delay", type=int, default=None, help="Delay between messages (ms)")
    parser.add_argument("--limit", type=int, default=None, help="Max transactions to send")
    args = parser.parse_args()

    stream_transactions(
        dataset_path=args.dataset,
        delay_ms=args.delay,
        limit=args.limit,
    )
