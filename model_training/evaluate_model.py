"""
Model Evaluation — Detailed analysis of the trained fraud detection model.
Loads the saved model, runs predictions on test data, and produces
a full evaluation report with confusion matrix and per-class metrics.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

from config import settings
from model_training.train_model import load_dataset, engineer_features


def create_spark_session():
    return (
        SparkSession.builder
        .appName(f"{settings.SPARK_APP_NAME}_Evaluation")
        .master(settings.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def evaluate_model(spark, dataset_path=None, model_path=None):
    """Run full model evaluation and print detailed report."""
    if dataset_path is None:
        dataset_path = settings.DATASET_PATH
    if model_path is None:
        model_path = settings.MODEL_PATH

    # Load model
    print(f"📂 Loading model from: {model_path}")
    model = PipelineModel.load(model_path)

    # Load and prepare data
    df = load_dataset(spark, dataset_path)
    df = engineer_features(df)

    # Use the same test split
    _, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"\n📦 Test set: {test_df.count()} rows")

    # Run predictions
    predictions = model.transform(test_df)

    # ── Metrics ──
    binary_eval = BinaryClassificationEvaluator(
        labelCol=settings.LABEL_COLUMN, rawPredictionCol="rawPrediction"
    )
    auc_roc = binary_eval.evaluate(predictions, {binary_eval.metricName: "areaUnderROC"})
    auc_pr = binary_eval.evaluate(predictions, {binary_eval.metricName: "areaUnderPR"})

    multi_eval = MulticlassClassificationEvaluator(
        labelCol=settings.LABEL_COLUMN, predictionCol="prediction"
    )
    accuracy = multi_eval.evaluate(predictions, {multi_eval.metricName: "accuracy"})
    f1 = multi_eval.evaluate(predictions, {multi_eval.metricName: "f1"})
    precision = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedPrecision"})
    recall = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedRecall"})

    # ── Confusion Matrix ──
    label = settings.LABEL_COLUMN
    tp = predictions.filter((col("prediction") == 1.0) & (col(label) == 1.0)).count()
    fp = predictions.filter((col("prediction") == 1.0) & (col(label) == 0.0)).count()
    tn = predictions.filter((col("prediction") == 0.0) & (col(label) == 0.0)).count()
    fn = predictions.filter((col("prediction") == 0.0) & (col(label) == 1.0)).count()

    print(f"\n{'=' * 60}")
    print(f"  📈 DETAILED MODEL EVALUATION REPORT")
    print(f"{'=' * 60}")
    print(f"\n  Classification Metrics:")
    print(f"  {'─' * 40}")
    print(f"  AUC-ROC:          {auc_roc:.4f}")
    print(f"  AUC-PR:           {auc_pr:.4f}")
    print(f"  Accuracy:         {accuracy:.4f}")
    print(f"  Precision (wt):   {precision:.4f}")
    print(f"  Recall (wt):      {recall:.4f}")
    print(f"  F1 Score (wt):    {f1:.4f}")

    print(f"\n  Confusion Matrix:")
    print(f"  {'─' * 40}")
    print(f"                  Predicted")
    print(f"                  Legit    Fraud")
    print(f"  Actual Legit  | {tn:>7}  {fp:>7}")
    print(f"  Actual Fraud  | {fn:>7}  {tp:>7}")

    fraud_precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    fraud_recall = tp / (tp + fn) if (tp + fn) > 0 else 0

    print(f"\n  Fraud-Specific Metrics:")
    print(f"  {'─' * 40}")
    print(f"  Fraud Precision:  {fraud_precision:.4f}")
    print(f"  Fraud Recall:     {fraud_recall:.4f}")
    if fraud_precision + fraud_recall > 0:
        fraud_f1 = 2 * fraud_precision * fraud_recall / (fraud_precision + fraud_recall)
        print(f"  Fraud F1:         {fraud_f1:.4f}")
    print(f"{'=' * 60}")

    return {
        "auc_roc": auc_roc,
        "auc_pr": auc_pr,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "fraud_precision": fraud_precision,
        "fraud_recall": fraud_recall,
    }


if __name__ == "__main__":
    spark = create_spark_session()
    try:
        dataset_path = sys.argv[1] if len(sys.argv) > 1 else None
        metrics = evaluate_model(spark, dataset_path)
    finally:
        spark.stop()
