"""
Spark ML Pipeline — Fraud Detection Model Training.

Trains a GBTClassifier (Gradient Boosted Trees) on the digital wallet transaction dataset.
Pipeline: Feature Engineering → StringIndexers → VectorAssembler → StandardScaler → GBT.
Saves the trained PipelineModel to disk for use in streaming predictions.

Dataset columns:
  transaction_id, customer_id, timestamp, channel, amount_src, device_id,
  new_device, ip_address, ip_country, location_mismatch, ip_risk_score,
  kyc_tier, account_age_days, device_trust_score, chargeback_history_count,
  risk_score_internal, txn_velocity_1h, txn_velocity_24h, corridor_risk, is_fraud
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, when, to_timestamp, upper,
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
    StandardScaler,
)
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

from config import settings


def create_spark_session():
    """Create a Spark session for model training."""
    return (
        SparkSession.builder
        .appName(f"{settings.SPARK_APP_NAME}_Training")
        .master(settings.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )


def load_dataset(spark, dataset_path):
    """Load the CSV dataset into a Spark DataFrame."""
    print(f"📂 Loading dataset from: {dataset_path}")

    df = spark.read.csv(
        dataset_path,
        header=True,
        inferSchema=True,
    )

    print(f"✅ Dataset loaded: {df.count()} rows, {len(df.columns)} columns")
    print(f"📋 Columns: {df.columns}")
    df.printSchema()

    return df


def engineer_features(df):
    """
    Apply feature engineering to the digital wallet transaction DataFrame.

    Transforms:
    - Boolean columns (new_device, location_mismatch) → integer flags
    - Timestamp → hour_of_day, day_of_week
    - Ensures numeric types for all feature columns
    """
    print("🔧 Engineering features...")

    # ── Parse timestamp and extract time features ──
    df = df.withColumn("ts_parsed", to_timestamp(col("timestamp")))
    df = df.withColumn("hour_of_day", hour(col("ts_parsed")).cast(IntegerType()))
    df = df.withColumn("day_of_week", dayofweek(col("ts_parsed")).cast(IntegerType()))

    # ── Boolean → Integer flags ──
    # new_device: "TRUE"/"FALSE" → 1/0
    df = df.withColumn(
        "new_device_flag",
        when(upper(col("new_device")) == "TRUE", 1).otherwise(0).cast(IntegerType())
    )

    # location_mismatch: "TRUE"/"FALSE" → 1/0
    df = df.withColumn(
        "location_mismatch_flag",
        when(upper(col("location_mismatch")) == "TRUE", 1).otherwise(0).cast(IntegerType())
    )

    # ── Cast numeric columns to DoubleType ──
    numeric_cols = [
        "amount_src", "ip_risk_score", "account_age_days",
        "device_trust_score", "chargeback_history_count",
        "risk_score_internal", "txn_velocity_1h", "txn_velocity_24h",
        "corridor_risk",
    ]
    for c in numeric_cols:
        df = df.withColumn(c, col(c).cast(DoubleType()))

    # ── Cast label to double ──
    df = df.withColumn(settings.LABEL_COLUMN, col(settings.LABEL_COLUMN).cast(DoubleType()))

    # ── Fill nulls in numeric columns ──
    df = df.fillna(0.0, subset=numeric_cols + ["hour_of_day", "day_of_week"])

    print("✅ Feature engineering complete")
    return df


def build_pipeline():
    """Build the full ML pipeline with indexers, assembler, scaler, and classifier."""

    # ── String Indexers for categoricals ──
    channel_indexer = StringIndexer(
        inputCol="channel", outputCol="channel_index", handleInvalid="keep"
    )
    country_indexer = StringIndexer(
        inputCol="ip_country", outputCol="ip_country_index", handleInvalid="keep"
    )
    kyc_indexer = StringIndexer(
        inputCol="kyc_tier", outputCol="kyc_tier_index", handleInvalid="keep"
    )

    # ── Assemble all features ──
    assembler = VectorAssembler(
        inputCols=settings.FEATURE_COLUMNS,
        outputCol="raw_features",
        handleInvalid="skip",
    )

    # ── Scale features ──
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True,
    )

    # ── Classifier: Gradient Boosted Trees ──
    classifier = GBTClassifier(
        labelCol=settings.LABEL_COLUMN,
        featuresCol="features",
        maxIter=50,
        maxDepth=6,
        stepSize=0.1,
        subsamplingRate=0.8,
        seed=42,
    )

    pipeline = Pipeline(stages=[
        channel_indexer,
        country_indexer,
        kyc_indexer,
        assembler,
        scaler,
        classifier,
    ])

    return pipeline


def train_and_save(spark, dataset_path=None):
    """Full training workflow: load → engineer → train → evaluate → save."""
    if dataset_path is None:
        dataset_path = settings.DATASET_PATH

    # ── Load ──
    df = load_dataset(spark, dataset_path)

    # ── Feature Engineering ──
    df = engineer_features(df)

    # ── Handle class imbalance info ──
    fraud_count = df.filter(col(settings.LABEL_COLUMN) == 1.0).count()
    legit_count = df.filter(col(settings.LABEL_COLUMN) == 0.0).count()
    total = fraud_count + legit_count
    print(f"\n📊 Class distribution:")
    print(f"   Legitimate: {legit_count} ({legit_count / total * 100:.2f}%)")
    print(f"   Fraud:      {fraud_count} ({fraud_count / total * 100:.2f}%)")

    # ── Train / Test Split ──
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"\n📦 Train: {train_df.count()} rows | Test: {test_df.count()} rows")

    # ── Build & Train Pipeline ──
    print("\n🚀 Training model...")
    pipeline = build_pipeline()
    model = pipeline.fit(train_df)

    # ── Evaluate ──
    predictions = model.transform(test_df)

    binary_evaluator = BinaryClassificationEvaluator(
        labelCol=settings.LABEL_COLUMN,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc_roc = binary_evaluator.evaluate(predictions)

    multi_evaluator = MulticlassClassificationEvaluator(
        labelCol=settings.LABEL_COLUMN,
        predictionCol="prediction",
    )
    accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
    f1 = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})
    precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
    recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})

    print(f"\n{'=' * 50}")
    print(f"📈 MODEL EVALUATION RESULTS (GBT)")
    print(f"{'=' * 50}")
    print(f"   Algorithm: Gradient Boosted Trees")
    print(f"   AUC-ROC:   {auc_roc:.4f}")
    print(f"   Accuracy:  {accuracy:.4f}")
    print(f"   Precision: {precision:.4f}")
    print(f"   Recall:    {recall:.4f}")
    print(f"   F1 Score:  {f1:.4f}")
    print(f"{'=' * 50}")

    # ── Save Model ──
    model_path = settings.MODEL_PATH
    print(f"\n💾 Saving model to: {model_path}")
    model.write().overwrite().save(model_path)
    print("✅ Model saved successfully!")

    # ── Save metrics to MongoDB ──
    metrics = {
        "auc_roc": auc_roc,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "train_size": train_df.count(),
        "test_size": test_df.count(),
        "fraud_count": fraud_count,
        "legit_count": legit_count,
        "total_count": total,
    }

    try:
        from database.mongo_client import MongoDBClient
        mongo = MongoDBClient()
        mongo.save_model_metrics(metrics)
        mongo.close()
        print("✅ Metrics saved to MongoDB")
    except Exception as e:
        print(f"⚠️  Could not save metrics to MongoDB: {e}")
        print("   (This is OK if MongoDB is not running yet)")

    return model, metrics


if __name__ == "__main__":
    spark = create_spark_session()
    try:
        dataset_path = sys.argv[1] if len(sys.argv) > 1 else None
        model, metrics = train_and_save(spark, dataset_path)
    finally:
        spark.stop()
