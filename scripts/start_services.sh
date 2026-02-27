#!/usr/bin/env bash
# ─────────────────────────────────────────────
# Start all infrastructure services via Docker Compose
# ─────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  🚀 Starting Fraud Detection Services"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

cd "$PROJECT_ROOT"

# Start Docker services
echo ""
echo "📦 Starting Docker containers..."
docker-compose up -d

# Wait for services to be ready
echo ""
echo "⏳ Waiting for services to be ready..."

# Wait for Kafka
echo -n "   Kafka: "
for i in $(seq 1 30); do
    if docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo "✅ Ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Timeout"
        exit 1
    fi
    sleep 2
done

# Wait for Redis
echo -n "   Redis: "
for i in $(seq 1 10); do
    if docker exec fraud-redis redis-cli ping &>/dev/null; then
        echo "✅ Ready"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "❌ Timeout"
        exit 1
    fi
    sleep 1
done

# Wait for MongoDB
echo -n "   MongoDB: "
for i in $(seq 1 10); do
    if docker exec fraud-mongodb mongosh --eval "db.adminCommand('ping')" &>/dev/null; then
        echo "✅ Ready"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "❌ Timeout"
        exit 1
    fi
    sleep 1
done

# Initialize MongoDB indexes
echo ""
echo "🔧 Initializing MongoDB..."
cd "$PROJECT_ROOT"
python3 database/init_db.py

# Create Kafka topic
echo ""
echo "📡 Creating Kafka topic..."
docker exec fraud-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic wallet_transactions \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ All services are running!"
echo ""
echo "  Kafka:    localhost:9092"
echo "  Redis:    localhost:6379"
echo "  MongoDB:  localhost:27017"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
