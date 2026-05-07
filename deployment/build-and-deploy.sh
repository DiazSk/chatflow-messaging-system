#!/usr/bin/env bash
# ============================================================================
# build-and-deploy.sh — ChatFlow build + EC2 deploy + post-deploy verification
#
# Builds gateway-server, message-processor, and the load-test client locally,
# uploads the JARs and DB schema to the appropriate EC2 hosts, then runs a
# remote health-check pass against the running stack.
#
# Required env (export before running):
#   SERVER_IP_1 SERVER_IP_2     gateway-server hosts behind the ALB
#   RABBITMQ_CONSUMER_IP        message-processor host (also receives schema)
#
# Optional env (used by the verification phase):
#   KEY                         SSH key path (default: ~/distributed_systems/chatflow-key.pem)
#   RABBITMQ_PUBLIC_IP          for rabbitmqctl ping (defaults to RABBITMQ_CONSUMER_IP)
#   MYSQL_PUBLIC_IP             for SHOW TABLES check
#   REDIS_PUBLIC_IP             for redis-cli PING check
#   CONSUMER_PUBLIC_IP          for /api/health check (defaults to RABBITMQ_CONSUMER_IP)
#   CLIENT_PUBLIC_IP            for client jar presence check
#   ALB_DNS                     for ALB-level smoke test
#   RABBITMQ_PRIVATE_IP         consumer→rabbitmq VPC IP
#   MYSQL_PRIVATE_IP            consumer→mysql VPC IP
#   REDIS_PRIVATE_IP            consumer→redis VPC IP
#   SKIP_VERIFY=1               skip the health-check phase
# ============================================================================

set -euo pipefail

KEY="${KEY:-$HOME/distributed_systems/chatflow-key.pem}"
SERVER_IP_1="${SERVER_IP_1:-<server-ip-1>}"
SERVER_IP_2="${SERVER_IP_2:-<server-ip-2>}"
RABBITMQ_CONSUMER_IP="${RABBITMQ_CONSUMER_IP:-<rabbitmq-consumer-ip>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ----------------------------------------------------------------------------
# Build phase
# ----------------------------------------------------------------------------

build_module() {
    local module="$1"
    echo "--- Building ${module} ---"
    (cd "${REPO_ROOT}/${module}" && mvn clean package -q)
}

build_all() {
    echo "=== Building all modules ==="
    build_module gateway-server
    build_module message-processor
    build_module client-part2
    echo "=== Build complete ==="
}

# ----------------------------------------------------------------------------
# Deploy phase
# ----------------------------------------------------------------------------

deploy_all() {
    echo "=== Deploying to EC2 instances ==="

    echo "--- Deploying message-processor to ${RABBITMQ_CONSUMER_IP} ---"
    scp -i "$KEY" \
        "${REPO_ROOT}/message-processor/target/message-processor-1.0-SNAPSHOT.jar" \
        "ec2-user@${RABBITMQ_CONSUMER_IP}:~/message-processor.jar"

    scp -i "$KEY" \
        "${REPO_ROOT}/database/01-init-schema.sql" \
        "${REPO_ROOT}/database/provision-db.sh" \
        "ec2-user@${RABBITMQ_CONSUMER_IP}:~/"

    for server_ip in "$SERVER_IP_1" "$SERVER_IP_2"; do
        echo "--- Deploying gateway-server to ${server_ip} ---"
        scp -i "$KEY" \
            "${REPO_ROOT}/gateway-server/target/gateway-server-1.0-SNAPSHOT.jar" \
            "ec2-user@${server_ip}:~/gateway-server.jar"
    done

    echo "=== Deployment complete ==="
}

# ----------------------------------------------------------------------------
# Verification phase: post-deploy health checks
# ----------------------------------------------------------------------------

verify_stack() {
    echo "=== Post-deploy verification ==="

    if [[ ! -f "$KEY" ]]; then
        echo "WARN: SSH key not found at $KEY — skipping verification" >&2
        return 0
    fi

    local server1_ip="${SERVER_IP_1}"
    local server2_ip="${SERVER_IP_2}"
    local rabbit_ip="${RABBITMQ_PUBLIC_IP:-$RABBITMQ_CONSUMER_IP}"
    local consumer_ip="${CONSUMER_PUBLIC_IP:-$RABBITMQ_CONSUMER_IP}"
    local mysql_ip="${MYSQL_PUBLIC_IP:-}"
    local redis_ip="${REDIS_PUBLIC_IP:-}"
    local client_ip="${CLIENT_PUBLIC_IP:-}"
    local alb_dns="${ALB_DNS:-}"
    local mysql_priv="${MYSQL_PRIVATE_IP:-}"
    local redis_priv="${REDIS_PRIVATE_IP:-}"

    local ssh_opts=(-i "$KEY" -o ConnectTimeout=20 -o StrictHostKeyChecking=accept-new -o BatchMode=yes)

    echo ""
    echo "--- Public HTTP probes ---"
    curl -sS -m 12 "http://${server1_ip}:8081/health" | head -c 200 || echo "server-1 :8081 FAIL"
    echo ""
    curl -sS -m 12 "http://${server2_ip}:8081/health" | head -c 200 || echo "server-2 :8081 FAIL"
    echo ""

    if [[ -n "$alb_dns" ]]; then
        curl -sS -m 12 -o /dev/null -w "ALB :8080 HTTP %{http_code}\n" "http://${alb_dns}:8080/" || true
        curl -sS -m 12 -o /dev/null -w "ALB :8081 HTTP %{http_code}\n" "http://${alb_dns}:8081/health" || true
    fi

    echo ""
    echo "--- RabbitMQ broker ---"
    ssh "${ssh_opts[@]}" "ec2-user@${rabbit_ip}" "sudo rabbitmqctl ping" || echo "FAIL rabbitmq"

    if [[ -n "$redis_ip" ]]; then
        echo ""
        echo "--- Redis (local PING) ---"
        ssh "${ssh_opts[@]}" "ec2-user@${redis_ip}" \
            "redis6-cli -h 127.0.0.1 -p 6379 PING 2>/dev/null || redis-cli -h 127.0.0.1 -p 6379 PING" \
            || echo "FAIL redis local"
    fi

    if [[ -n "$mysql_ip" ]]; then
        echo ""
        echo "--- MySQL (schema check) ---"
        if ssh "${ssh_opts[@]}" "ec2-user@${mysql_ip}" \
                "mysql -u chatflow -p'<DB_PASSWORD>' chatflow -e 'SHOW TABLES;'"; then
            ssh "${ssh_opts[@]}" "ec2-user@${mysql_ip}" \
                "mysql -u chatflow -p'<DB_PASSWORD>' chatflow -N -e 'SELECT COUNT(*) AS messages FROM messages;'"
        else
            echo "SKIP/FAIL: cannot SSH to MySQL host or query failed (check SG rules)."
        fi
    fi

    echo ""
    echo "--- Consumer (message-processor) ---"
    if [[ -n "$redis_priv" ]]; then
        ssh "${ssh_opts[@]}" "ec2-user@${consumer_ip}" \
            "redis6-cli -h ${redis_priv} -p 6379 PING 2>/dev/null || redis-cli -h ${redis_priv} -p 6379 PING" \
            || echo "FAIL redis from consumer"
    fi

    if [[ -n "$mysql_priv" ]]; then
        ssh "${ssh_opts[@]}" "ec2-user@${consumer_ip}" \
            "mysql -u chatflow -p'<DB_PASSWORD>' -h ${mysql_priv} chatflow -e 'SELECT 1 AS ok;'" \
            || echo "FAIL MySQL from consumer (check SG 3306 from VPC CIDR; FLUSH HOSTS if ERROR 1129)"
    fi

    ssh "${ssh_opts[@]}" "ec2-user@${consumer_ip}" \
        "curl -sS -m 8 http://127.0.0.1:9091/api/health || echo METRICS_DOWN"
    ssh "${ssh_opts[@]}" "ec2-user@${consumer_ip}" \
        "pgrep -af message-processor.jar || true"

    if [[ -n "$client_ip" ]]; then
        echo ""
        echo "--- Client (load-test driver) ---"
        ssh "${ssh_opts[@]}" "ec2-user@${client_ip}" \
            "ls -la ~/client-v3.jar 2>/dev/null || echo missing client-v3.jar"
    fi

    echo ""
    echo "=== Verification complete ==="
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

build_all
deploy_all

if [[ "${SKIP_VERIFY:-0}" != "1" ]]; then
    verify_stack
fi

echo ""
echo "Next steps:"
echo "  1. SSH ${RABBITMQ_CONSUMER_IP} and run: DB_PASSWORD=... bash provision-db.sh"
echo "  2. Start message-processor:           bash run-message-processor.sh"
echo "  3. Start gateway-server on each app host"
echo "  4. Drive load with load-testing/run-benchmarks.sh"
