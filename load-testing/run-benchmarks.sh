#!/usr/bin/env bash
# ============================================================================
# run-benchmarks.sh — ChatFlow benchmark harness
#
# Unified entry point for write-pressure load tests, batch-size calibration,
# JMeter API-read scenarios, and result downloads.
#
# Usage:
#   ./run-benchmarks.sh --baseline           [SERVER_URI] [METRICS_URL]
#   ./run-benchmarks.sh --stress             [SERVER_URI] [METRICS_URL]
#   ./run-benchmarks.sh --endurance          [SERVER_URI] [METRICS_URL]
#   ./run-benchmarks.sh --batch-experiment
#   ./run-benchmarks.sh --jmeter <plan.jmx>
#   ./run-benchmarks.sh --download <client-public-ip>
#
# Environment overrides (relevant subsets per command):
#   KEY                    SSH key path (default: ~/distributed_systems/chatflow-key.pem)
#   MYSQL_PUBLIC_IP        chatflow-mysql public IP (batch-experiment)
#   CONSUMER_PUBLIC_IP     chatflow-consumer public IP (batch-experiment)
#   CLIENT_PUBLIC_IP       chatflow-client public IP (batch-experiment)
#   ALB_HOST               ALB DNS name (jmeter, batch-experiment)
#   CONSUMER_HOST          consumer host for jmeter writes (default: localhost)
#   CONSUMER_PORT          metrics API port (default: 9091)
#   RUNID                  jmeter run identifier (e.g. B0-S1)
#   JMETER_HOME            JMeter install dir (default: ~/apache-jmeter-5.6.3)
#   ARTIFACTS_DIR          jmeter output dir (default: ./artifacts)
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_JAR_DEFAULT="${SCRIPT_DIR}/custom-client/target/chatflow-client-1.0-SNAPSHOT.jar"
CLIENT_JAR="${CLIENT_JAR:-$CLIENT_JAR_DEFAULT}"
KEY="${KEY:-$HOME/distributed_systems/chatflow-key.pem}"

# ----------------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------------

log() {
    printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

require_client_jar() {
    if [[ ! -f "$CLIENT_JAR" ]]; then
        echo "ERROR: client jar not found at $CLIENT_JAR" >&2
        echo "Build it first: mvn -f custom-client/pom.xml clean package -DskipTests" >&2
        exit 1
    fi
}

reset_local_db() {
    if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -q '^chatflow-mysql$'; then
        log "Resetting local docker chatflow-mysql..."
        docker exec chatflow-mysql mysql -uchatflow -pChatFlow@2026 chatflow \
            -e "TRUNCATE TABLE messages; TRUNCATE TABLE dead_letter_messages;" 2>/dev/null || true
    fi
}

snapshot_metrics() {
    local metrics_url="$1"
    local out_dir="$2"
    curl -s "$metrics_url/api/metrics" > "$out_dir/final-metrics.json" 2>/dev/null || true
    curl -s "$metrics_url/api/all" > "$out_dir/final-all.json" 2>/dev/null || true
}

archive_results() {
    local label="$1"
    local log_file="$2"
    local metrics_url="$3"
    local results_dir="results-${label}-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$results_dir"
    mv -f metrics.csv "$results_dir/" 2>/dev/null || true
    mv -f throughput.csv "$results_dir/" 2>/dev/null || true
    mv -f metrics-api-results-*.log "$results_dir/" 2>/dev/null || true
    snapshot_metrics "$metrics_url" "$results_dir"
    cp -f "$log_file" "$results_dir/" 2>/dev/null || true
    log "Results archived: $results_dir/"
}

# ----------------------------------------------------------------------------
# Test profiles: write-pressure tests against the gateway via WebSocket
# ----------------------------------------------------------------------------

run_write_test() {
    local label="$1"
    local total_messages="$2"
    local server_uri="$3"
    local metrics_url="$4"

    require_client_jar

    local log_file="${label}-full-output-$(date +%Y%m%d-%H%M%S).log"
    exec > >(tee -a "$log_file") 2>&1

    log "=== ChatFlow ${label} run ==="
    log "Server:      $server_uri"
    log "Metrics:     $metrics_url"
    log "Total:       $total_messages messages"
    log "Output log:  $log_file"

    reset_local_db

    java -Xmx1g -jar "$CLIENT_JAR" "$server_uri" "$metrics_url" "$total_messages"

    log "${label} run complete: $(date)"
    archive_results "$label" "$log_file" "$metrics_url"
}

run_baseline() {
    run_write_test "baseline" 500000 "${1:-ws://localhost:8080/chat/}" "${2:-http://localhost:9091}"
}

run_stress() {
    run_write_test "stress" 1000000 "${1:-ws://localhost:8080/chat/}" "${2:-http://localhost:9091}"
}

run_endurance() {
    local server_uri="${1:-ws://localhost:8080/chat/}"
    local metrics_url="${2:-http://localhost:9091}"
    local rounds="${ROUNDS:-5}"
    local per_round="${PER_ROUND:-500000}"

    require_client_jar

    local log_file="endurance-full-output-$(date +%Y%m%d-%H%M%S).log"
    exec > >(tee -a "$log_file") 2>&1

    log "=== ChatFlow endurance run ($rounds rounds × $per_round messages) ==="
    log "Server:  $server_uri"
    log "Metrics: $metrics_url"

    for round in $(seq 1 "$rounds"); do
        log ""
        log "================ Endurance round $round of $rounds ================"
        java -Xmx1g -jar "$CLIENT_JAR" "$server_uri" "$metrics_url" "$per_round"

        log "--- Metrics snapshot after round $round ---"
        curl -s "$metrics_url/api/metrics" | python3 -m json.tool 2>/dev/null \
            || log "(metrics API unreachable)"

        if [[ "$round" -lt "$rounds" ]]; then
            log "Cooldown 30s before round $((round + 1))..."
            sleep 30
        fi
    done

    log ""
    log "=== Endurance complete: $((rounds * per_round)) messages sent ==="
    archive_results "endurance" "$log_file" "$metrics_url"
}

# ----------------------------------------------------------------------------
# Batch-size calibration: orchestrates remote restarts of message-processor
# with varying WRITE_BATCH_SIZE values and records peak write rate.
# ----------------------------------------------------------------------------

run_batch_experiment() {
    : "${KEY:?KEY env var required (path to SSH key)}"
    : "${MYSQL_PUBLIC_IP:?MYSQL_PUBLIC_IP env var required}"
    : "${CONSUMER_PUBLIC_IP:?CONSUMER_PUBLIC_IP env var required}"
    : "${CLIENT_PUBLIC_IP:?CLIENT_PUBLIC_IP env var required}"

    local alb_host="${ALB_HOST:-chatflow-alb-1805851036.us-west-2.elb.amazonaws.com}"
    local alb_uri="ws://${alb_host}:8080/chat/"
    local consumer_private_ip="${CONSUMER_PRIVATE_IP:-172.31.19.61}"
    local sample_messages="${SAMPLE_MESSAGES:-100000}"
    local batch_sizes=(${BATCH_SIZES:-100 500 1000 5000})

    log "=== Batch-size calibration ==="
    log "ALB:           $alb_uri"
    log "Consumer:      $consumer_private_ip"
    log "Sample size:   $sample_messages msgs/batch"
    log "Batch sizes:   ${batch_sizes[*]}"

    for batch in "${batch_sizes[@]}"; do
        log ""
        log "--- WRITE_BATCH_SIZE=$batch ---"

        ssh -i "$KEY" "ec2-user@${MYSQL_PUBLIC_IP}" \
            "mysql -u chatflow -p'ChatFlow@2026' chatflow -e 'TRUNCATE TABLE messages; TRUNCATE TABLE dead_letter_messages;'"

        # bash -s + heredoc avoids pkill matching the SSH shell's own argv.
        ssh -i "$KEY" "ec2-user@${CONSUMER_PUBLIC_IP}" bash -s -- "$batch" <<'REMOTE'
BATCH="$1"
pkill -f message-processor.jar || true
sleep 3
export RABBITMQ_HOST=172.31.29.217 RABBITMQ_USER=admin RABBITMQ_PASS=password123
export DB_HOST=172.31.24.182 DB_USER=chatflow DB_PASS='ChatFlow@2026' DB_NAME=chatflow
export DB_POOL_SIZE=15 WRITE_BUFFER_CAPACITY=1500000 WRITE_BATCH_SIZE="$BATCH"
export WRITER_THREADS=5 CONSUMER_THREADS=4 PREFETCH_COUNT=100
nohup java -Xmx512m -Xms256m -XX:+UseG1GC -jar ~/message-processor.jar \
  > ~/message-processor.log 2>&1 &
REMOTE
        sleep 6

        ssh -i "$KEY" "ec2-user@${CLIENT_PUBLIC_IP}" \
            "java -Xmx512m -jar ~/client-v3.jar '${alb_uri}' 'http://${consumer_private_ip}:9091' ${sample_messages}"

        sleep 15

        local result
        result=$(ssh -i "$KEY" "ec2-user@${CLIENT_PUBLIC_IP}" \
            "curl -s http://${consumer_private_ip}:9091/api/metrics")

        echo "$result" > "batch_result_${batch}.json"
        echo "$result" | python3 -c "
import sys, json
d = json.load(sys.stdin)
t = d['throughput']; l = d['latency']
print(f\"  peakWriteRate={t['peakWriteRate']} msg/s  avgBatchMs={l['avgBatchWriteMs']}  maxBatchMs={l['maxBatchWriteMs']}\")
"
    done

    log ""
    log "=== Batch-size calibration complete ==="
    log "Per-batch metrics saved to batch_result_*.json"
}

# ----------------------------------------------------------------------------
# JMeter harness: API read-load scenarios (S1, S2, E1)
# ----------------------------------------------------------------------------

run_jmeter() {
    local plan="${1:?run_jmeter requires a plan file (e.g. S1-baseline.jmx)}"
    : "${RUNID:?RUNID env var required (e.g. B0-S1)}"

    local consumer_host="${CONSUMER_HOST:-localhost}"
    local consumer_port="${CONSUMER_PORT:-9091}"
    local alb_host="${ALB_HOST:-localhost}"
    local jmeter_home="${JMETER_HOME:-$HOME/apache-jmeter-5.6.3}"
    local artifacts_dir="${ARTIFACTS_DIR:-./artifacts}"
    local jmeter="$jmeter_home/bin/jmeter"

    if [[ ! -x "$jmeter" ]]; then
        echo "ERROR: JMeter not found at $jmeter" >&2
        echo "Install JMeter 5.6.3 and set JMETER_HOME, or:" >&2
        echo "  wget https://downloads.apache.org/jmeter/binaries/apache-jmeter-5.6.3.tgz" >&2
        echo "  tar -xzf apache-jmeter-5.6.3.tgz -C ~/" >&2
        exit 1
    fi

    mkdir -p "$artifacts_dir"

    local jtl_file="$artifacts_dir/$RUNID.jtl"
    local report_dir="$artifacts_dir/report-$RUNID"
    local metrics_prefix="$artifacts_dir/$RUNID-metrics"

    log "=== JMeter run: $RUNID ==="
    log "Plan:     $plan"
    log "Consumer: $consumer_host:$consumer_port"
    log "ALB:      $alb_host"
    log "JTL:      $jtl_file"
    log "Report:   $report_dir"

    log "Snapshotting metrics API (start)..."
    if curl -sf --max-time 5 "http://$consumer_host:$consumer_port/api/all" \
            > "${metrics_prefix}-start.json" 2>/dev/null; then
        log "  saved ${metrics_prefix}-start.json"
    else
        log "  WARN: metrics API unreachable, continuing"
    fi

    log "Starting JMeter..."
    "$jmeter" -n \
        -t "$SCRIPT_DIR/jmeter/$plan" \
        -l "$jtl_file" \
        -e -o "$report_dir" \
        -Jconsumer.host="$consumer_host" \
        -Jconsumer.port="$consumer_port" \
        -Jalb.host="$alb_host" \
        -Jjtl.file="$jtl_file" \
        2>&1 | tee "$artifacts_dir/$RUNID-jmeter.log" &
    local jmeter_pid=$!

    local plan_duration
    plan_duration=$(grep -oP 'ThreadGroup\.duration.*?<stringProp[^>]*>\K[0-9]+' \
        "$SCRIPT_DIR/jmeter/$plan" 2>/dev/null | head -1 || echo "300")
    local mid_wait=$(( plan_duration / 2 ))

    log "Waiting ${mid_wait}s for mid-run snapshot..."
    sleep "$mid_wait"

    log "Snapshotting metrics API (mid)..."
    curl -sf --max-time 10 "http://$consumer_host:$consumer_port/api/all" \
        > "${metrics_prefix}-mid.json" 2>/dev/null \
        && log "  saved ${metrics_prefix}-mid.json" \
        || log "  WARN: mid-run snapshot failed"

    wait "$jmeter_pid"
    local jmeter_exit=$?

    log "Snapshotting metrics API (end)..."
    curl -sf --max-time 10 "http://$consumer_host:$consumer_port/api/all" \
        > "${metrics_prefix}-end.json" 2>/dev/null \
        && log "  saved ${metrics_prefix}-end.json" \
        || log "  WARN: end-run snapshot failed"

    log "JMeter run complete: $RUNID (exit $jmeter_exit)"
    log "HTML report: $report_dir/index.html"

    if [[ -f "$jtl_file" ]]; then
        echo ""
        echo "--- Summary row ---"
        JTL_FILE="$jtl_file" RUNID="$RUNID" python3 - <<'PYEOF'
import csv, os, statistics, sys

jtl = os.environ["JTL_FILE"]
runid = os.environ["RUNID"]

rows = []
with open(jtl) as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            rows.append({
                'elapsed': int(row['elapsed']),
                'success': row['success'].strip().lower() == 'true',
            })
        except (KeyError, ValueError):
            pass

if not rows:
    print("  (no data in JTL)")
    sys.exit(0)

elapsed = sorted(r['elapsed'] for r in rows)
errors = [r for r in rows if not r['success']]
n = len(elapsed)
avg_ms = statistics.mean(elapsed)
p95 = elapsed[int(n * 0.95)]
p99 = elapsed[int(n * 0.99)]
err_pct = len(errors) / n * 100

with open(jtl) as f:
    reader = csv.DictReader(f)
    timestamps = [int(r['timeStamp']) for r in reader if r.get('timeStamp', '').isdigit()]

duration_s = (max(timestamps) - min(timestamps)) / 1000 if len(timestamps) > 1 else 1
rps = n / duration_s

print(f"  Run:     {runid}")
print(f"  Samples: {n}")
print(f"  Avg ms:  {avg_ms:.1f}")
print(f"  p95 ms:  {p95}")
print(f"  p99 ms:  {p99}")
print(f"  RPS:     {rps:.1f}")
print(f"  Err %:   {err_pct:.2f}%")
print(f"  Row:     | {runid} | {avg_ms:.0f} | {p95} | {p99} | {rps:.0f} | {err_pct:.2f}% |")
PYEOF
    fi

    return "$jmeter_exit"
}

# ----------------------------------------------------------------------------
# Download artifacts: pulls result directories and stray logs from a remote
# client EC2 to the local working directory.
# ----------------------------------------------------------------------------

download_results() {
    local client_ip="${1:?download_results requires client public IP}"
    local local_dir="${SCRIPT_DIR}"

    log "=== Downloading results from ec2-user@${client_ip} ==="

    log "Available remote artifacts:"
    ssh -i "$KEY" "ec2-user@${client_ip}" \
        'ls -d results-* test*-full-output-*.log metrics-api-results-*.log 2>/dev/null || echo "(none in home dir)"'

    log "Pulling files..."
    scp -i "$KEY" -r "ec2-user@${client_ip}":~/results-* "$local_dir/" 2>/dev/null \
        && log "  pulled results-* directories" \
        || log "  no results-* directories"

    scp -i "$KEY" "ec2-user@${client_ip}":~/test*-full-output-*.log "$local_dir/" 2>/dev/null \
        && log "  pulled output logs" \
        || log "  no stray output logs"

    scp -i "$KEY" "ec2-user@${client_ip}":~/metrics-api-results-*.log "$local_dir/" 2>/dev/null \
        && log "  pulled metrics API logs" \
        || log "  no stray metrics logs"

    scp -i "$KEY" "ec2-user@${client_ip}":~/metrics.csv "$local_dir/" 2>/dev/null || true
    scp -i "$KEY" "ec2-user@${client_ip}":~/throughput.csv "$local_dir/" 2>/dev/null || true

    log "Download complete: $local_dir/"
}

# ----------------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------------

usage() {
    sed -n '2,30p' "$0"
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

cmd="$1"; shift || true

case "$cmd" in
    --baseline)         run_baseline "$@" ;;
    --stress)           run_stress "$@" ;;
    --endurance)        run_endurance "$@" ;;
    --batch-experiment) run_batch_experiment "$@" ;;
    --jmeter)           run_jmeter "$@" ;;
    --download)         download_results "$@" ;;
    -h|--help)          usage ;;
    *)
        echo "Unknown command: $cmd" >&2
        usage
        ;;
esac
