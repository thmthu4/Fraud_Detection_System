"""
Spark Structured Streaming — Real-Time Fraud Detection Consumer.

Subscribes to the Kafka topic, applies the trained ML model for
fraud prediction, and writes results to Redis (alerts) and MongoDB (persistence).

Adapted for the Digital Wallet Transaction dataset schema.
"""

import sys
import json
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, udf, when, lit, current_timestamp,
    hour, dayofweek, to_timestamp, upper,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType,
)
from pyspark.ml import PipelineModel

from config import settings


def create_spark_session():
    """Create Spark session with Kafka + streaming support."""
    return (
        SparkSession.builder
        .appName(f"{settings.SPARK_APP_NAME}_Streaming")
        .master(settings.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )


# Schema matching the Kafka producer JSON messages (streaming_data.csv)
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ingestion_time", DoubleType(), True),
    StructField("channel", StringType(), True),
    StructField("amount_src", DoubleType(), True),
    StructField("new_device", StringType(), True),
    StructField("ip_country", StringType(), True),
    StructField("location_mismatch", StringType(), True),
    StructField("ip_risk_score", DoubleType(), True),
    StructField("kyc_tier", StringType(), True),
    StructField("account_age_days", IntegerType(), True),
    StructField("device_trust_score", DoubleType(), True),
    StructField("chargeback_history_count", IntegerType(), True),
    StructField("risk_score_internal", DoubleType(), True),
    StructField("txn_velocity_1h", IntegerType(), True),
    StructField("txn_velocity_24h", IntegerType(), True),
    StructField("corridor_risk", IntegerType(), True),
    StructField("is_fraud", IntegerType(), True),
])


def engineer_streaming_features(df):
    """Apply the same feature engineering as training to the streaming data."""

    # Parse timestamp and extract time features
    df = df.withColumn("ts_parsed", to_timestamp(col("timestamp")))
    df = df.withColumn("hour_of_day", hour(col("ts_parsed")).cast(IntegerType()))
    df = df.withColumn("day_of_week", dayofweek(col("ts_parsed")).cast(IntegerType()))

    # Boolean → Integer flags
    df = df.withColumn(
        "new_device_flag",
        when(upper(col("new_device")) == "TRUE", 1).otherwise(0).cast(IntegerType())
    )
    df = df.withColumn(
        "location_mismatch_flag",
        when(upper(col("location_mismatch")) == "TRUE", 1).otherwise(0).cast(IntegerType())
    )

    # Cast numeric columns
    numeric_cols = [
        "amount_src", "ip_risk_score", "account_age_days",
        "device_trust_score", "chargeback_history_count",
        "risk_score_internal", "txn_velocity_1h", "txn_velocity_24h",
        "corridor_risk",
    ]
    for c in numeric_cols:
        df = df.withColumn(c, col(c).cast(DoubleType()))

    # Cast label
    df = df.withColumn(settings.LABEL_COLUMN, col(settings.LABEL_COLUMN).cast(DoubleType()))

    # Fill nulls
    df = df.fillna(0.0, subset=numeric_cols + ["hour_of_day", "day_of_week"])

    return df


def process_batch(batch_df, batch_id, model):
    """
    Process each micro-batch:
    1. Apply feature engineering
    2. Run ML model prediction (pipeline includes indexers + assembler + scaler + classifier)
    3. Write fraud alerts to Redis
    4. Write all results to MongoDB
    """
    if batch_df.count() == 0:
        return

    print(f"\n{'─' * 50}")
    print(f"📦 Processing batch {batch_id}: {batch_df.count()} transactions")

    start_time = time.time()

    # ── Feature Engineering ──
    featured_df = engineer_streaming_features(batch_df)

    # ── ML Prediction (pipeline already includes StringIndexers) ──
    predictions = model.transform(featured_df)

    # Extract fraud probability from probability vector [prob_class_0, prob_class_1]
    extract_fraud_prob = udf(
        lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0,
        DoubleType(),
    )
    predictions = predictions.withColumn(
        "fraud_probability", extract_fraud_prob(col("probability"))
    )
    predictions = predictions.withColumn("processed_at", lit(datetime.utcnow().isoformat()))

    processing_time = time.time() - start_time

    # ── Collect results ──
    results = predictions.select(
        "transaction_id", "customer_id", "username", "email",
        "timestamp", "channel",
        "amount_src", "ip_country", "ip_risk_score", "kyc_tier",
        "account_age_days", "device_trust_score",
        "new_device_flag", "location_mismatch_flag",
        "chargeback_history_count", "risk_score_internal",
        "txn_velocity_1h", "txn_velocity_24h", "corridor_risk",
        "prediction", "fraud_probability", "processed_at",
        "is_fraud",
    ).collect()

    # ── Write to Redis + MongoDB ──
    try:
        from feature_store.redis_client import RedisFeatureStore
        redis_store = RedisFeatureStore()

        from database.mongo_client import MongoDBClient
        mongo = MongoDBClient()

        fraud_count = 0
        transactions_to_insert = []

        for row in results:
            tx_data = row.asDict()

            # Convert any non-serializable types
            for k, v in tx_data.items():
                if hasattr(v, "item"):
                    tx_data[k] = v.item()

            transactions_to_insert.append(tx_data)

            # Update Redis counters
            redis_store.increment_transaction_count()
            redis_store.store_recent_transaction(tx_data)

            # Update user stats
            redis_store.update_user_stats(
                tx_data.get("customer_id", "unknown"),
                tx_data.get("amount_src", 0),
                is_fraud=(tx_data.get("prediction", 0) == 1.0),
            )

            # Fraud alert
            if tx_data.get("prediction", 0) == 1.0:
                fraud_count += 1
                redis_store.increment_fraud_count()
                redis_store.store_fraud_alert(
                    tx_data["transaction_id"],
                    tx_data,
                )

        # Batch insert to MongoDB
        if transactions_to_insert:
            mongo.insert_transactions_batch(transactions_to_insert)

        redis_store.close()
        mongo.close()

        print(f"   ✅ Batch {batch_id} processed in {processing_time:.2f}s")
        print(f"   📊 Total: {len(results)} | 🚨 Fraud: {fraud_count}")

    except Exception as e:
        print(f"   ⚠️  Error writing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()


def start_streaming(model_path=None):
    """Start the Spark Structured Streaming pipeline."""
    if model_path is None:
        model_path = settings.MODEL_PATH

    print(f"{'=' * 60}")
    print(f"  🚀 FRAUD DETECTION — SPARK STREAMING PIPELINE")
    print(f"{'=' * 60}")
    print(f"  Kafka:  {settings.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Topic:  {settings.KAFKA_TOPIC}")
    print(f"  Model:  {model_path}")
    print(f"{'=' * 60}")

    spark = create_spark_session()

    # ── Load the trained model (full pipeline: indexers+assembler+scaler+classifier) ──
    print(f"\n📂 Loading ML model from: {model_path}")
    model = PipelineModel.load(model_path)
    print("✅ Model loaded successfully")

    # ── Subscribe to Kafka ──
    print(f"\n📡 Subscribing to Kafka topic: {settings.KAFKA_TOPIC}")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", settings.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON messages ──
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
    )

    # ── Process via foreachBatch ──
    query = (
        parsed_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, model)
        )
        .outputMode("update")
        .option("checkpointLocation", "/tmp/fraud_detection_checkpoint")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("\n✅ Streaming started! Waiting for transactions...")
    print("   Press Ctrl+C to stop.\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping stream...")
        query.stop()
        spark.stop()
        print("✅ Stream stopped gracefully")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Spark Streaming Fraud Detector")
    parser.add_argument("--model", type=str, default=None, help="Path to saved model")
    args = parser.parse_args()

    start_streaming(model_path=args.model)
