#!/usr/bin/env bash
# ─────────────────────────────────────────────
# Run the full fraud detection pipeline end-to-end
# ─────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Auto-activate virtual environment
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Fraud Detection — Full Pipeline"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

cd "$PROJECT_ROOT"

# Check if dataset exists
DATASET=${1:-data/dataset.csv}
if [ ! -f "$DATASET" ]; then
    echo ""
    echo "❌ Dataset not found: $DATASET"
    echo "   Usage: ./scripts/run_pipeline.sh [path/to/dataset.csv]"
    echo "   Place your dataset CSV in the data/ directory."
    exit 1
fi

RETRAIN=false
if [[ "$*" == *"--retrain"* ]]; then
    RETRAIN=true
fi

echo ""
echo "📂 Dataset: $DATASET"

# ── Step 1: Train Model (skip if already trained) ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Step 1/4: ML Model"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -d "models/fraud_model" ] && [ "$RETRAIN" = false ]; then
    echo "✅ Model already exists at models/fraud_model — skipping training."
    echo "   (Use --retrain flag to force retraining)"
else
    echo "🚀 Training model..."
    python3 model_training/train_model.py "$DATASET"
fi

# ── Step 2: Ensure Kafka topic exists ──
echo ""
echo "📡 Ensuring Kafka topic exists..."
docker exec fraud-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic wallet_transactions \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || \
sudo docker exec fraud-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic wallet_transactions \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || echo "⚠️  Topic may already exist"
echo "✅ Kafka topic ready"

# Clean up old checkpoint
rm -rf /tmp/fraud_detection_checkpoint

# ── Step 3: Start Spark Streaming (background) ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Step 3/4: Starting Spark Streaming Consumer"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 spark_streaming/stream_processor.py &
STREAM_PID=$!
echo "   PID: $STREAM_PID"

# Wait for stream processor to initialize
sleep 15

# ── Step 3: Start Kafka Producer (background) ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Step 3/4: Starting Kafka Producer"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 kafka_producer/producer.py --dataset "data/streaming_data.csv" --delay 50 &
PRODUCER_PID=$!
echo "   PID: $PRODUCER_PID"

# ── Step 4: Launch Streamlit Dashboard ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Step 4/4: Launching Streamlit Dashboard"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  🌐 Dashboard: http://localhost:8501"
echo ""
echo "  Press Ctrl+C to stop all services"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Trap Ctrl+C to clean up
cleanup() {
    echo ""
    echo "⏹️  Stopping pipeline..."
    kill $STREAM_PID 2>/dev/null || true
    kill $PRODUCER_PID 2>/dev/null || true
    echo "✅ Pipeline stopped"
    exit 0
}
trap cleanup SIGINT SIGTERM

streamlit run dashboard/app.py --server.port 8501 --server.headless true
