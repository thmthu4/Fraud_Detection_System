#!/usr/bin/env bash
# ─────────────────────────────────────────────
# Stop all Docker services and clean up
# ─────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "⏹️  Stopping all services..."

cd "$PROJECT_ROOT"
docker-compose down

# Clean up checkpoint
rm -rf /tmp/fraud_detection_checkpoint

echo "✅ All services stopped and cleaned up"
