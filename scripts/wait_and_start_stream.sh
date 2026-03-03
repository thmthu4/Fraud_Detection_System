#!/usr/bin/env bash
# Wait for the trained model to be available before starting stream processor
set -e

MODEL_PATH="${MODEL_PATH:-/app/models/fraud_model}"
MAX_WAIT=120
WAITED=0

echo "Waiting for trained model at $MODEL_PATH ..."

while [ ! -d "$MODEL_PATH" ] || [ ! -f "$MODEL_PATH/metadata/part-00000" ]; do
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "ERROR: Model not found after ${MAX_WAIT}s. Training may have failed."
        exit 1
    fi
    sleep 5
    WAITED=$((WAITED + 5))
    echo "  Still waiting... (${WAITED}s)"
done

echo "Model found. Waiting 10s for Kafka to stabilize..."
sleep 10

echo "Starting stream processor..."
exec python3 spark_streaming/stream_processor.py
