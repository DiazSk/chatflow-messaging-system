#!/usr/bin/env bash
# ============================================================
# Run ChatFlow message-processor on an EC2 host.
#
# Optimization profiles (set PROFILE before invoking):
#   PROFILE=B0   → baseline, no optimizations (default)
#   PROFILE=O1   → Redis read-through cache
#   PROFILE=O2   → summary tables for analytics
#   PROFILE=O12  → Redis + summary tables
#
# Usage:
#   PROFILE=O12 ./run-message-processor.sh
# ============================================================

set -euo pipefail

# Kill any existing consumer process
pkill -f message-processor.jar || true
sleep 2

# ============ INFRASTRUCTURE ============
export RABBITMQ_HOST="<RABBITMQ_PRIVATE_IP>"
export RABBITMQ_PORT="5672"
export RABBITMQ_USER="<RABBITMQ_USER>"
export RABBITMQ_PASS="<RABBITMQ_PASSWORD>"
export CONSUMER_THREADS="4"
export PREFETCH_COUNT="500"
export BROADCAST_WS_PORT="9090"
export METRICS_API_PORT="9091"

# ============ DATABASE (remote chatflow-mysql) ============
export DB_HOST="<MYSQL_PRIVATE_IP>"
export DB_PORT="3306"
export DB_NAME="chatflow"
export DB_USER="chatflow"
export DB_PASS="<DB_PASSWORD>"
export DB_POOL_SIZE="15"

# ============ WRITE PIPELINE (optimized for EC2 t3.micro) ============
export WRITE_BUFFER_CAPACITY="1500000"
export WRITE_BATCH_SIZE="5000"
export WRITER_THREADS="5"
export FLUSH_INTERVAL_MS="500"

# ============ CIRCUIT BREAKER ============
export CB_FAILURE_THRESHOLD="5"
export CB_RESET_TIMEOUT_MS="15000"

# ============ OPTIMIZATION FLAGS ============
PROFILE="${PROFILE:-B0}"
EXTRA_JAVA_FLAGS=""
# Dedicated chatflow-redis private IP (override via env when topology changes)
REDIS_HOST="${REDIS_HOST:-<REDIS_PRIVATE_IP>}"
REDIS_PORT="${REDIS_PORT:-6379}"

case "$PROFILE" in
  O1)  EXTRA_JAVA_FLAGS="-Dredis.host=$REDIS_HOST -Dredis.port=$REDIS_PORT" ;;
  O2)  EXTRA_JAVA_FLAGS="-Dsummary.tables=true" ;;
  O12) EXTRA_JAVA_FLAGS="-Dredis.host=$REDIS_HOST -Dredis.port=$REDIS_PORT -Dsummary.tables=true" ;;
  B0|*) EXTRA_JAVA_FLAGS="" ;;
esac

echo "============================================"
echo "  ChatFlow message-processor — profile: $PROFILE"
echo "  DB: $DB_HOST:$DB_PORT/$DB_NAME (pool=$DB_POOL_SIZE)"
echo "  Buffer: $WRITE_BUFFER_CAPACITY | Batch: $WRITE_BATCH_SIZE | Writers: $WRITER_THREADS"
echo "  Flags: ${EXTRA_JAVA_FLAGS:-none}"
echo "============================================"

nohup java -Xmx512m -Xms256m \
    -XX:+UseG1GC \
    ${EXTRA_JAVA_FLAGS} \
    -jar ~/message-processor.jar \
    > ~/message-processor.log 2>&1 &

echo "message-processor started (PID: $!)"
echo "Log:     tail -f ~/message-processor.log"
echo "Metrics: curl http://localhost:9091/api/health"
