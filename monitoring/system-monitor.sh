#!/usr/bin/env bash
# ============================================================================
# system-monitor.sh — ChatFlow unified live dashboard
#
# Polls the message-processor MetricsAPI (queue depth, write rate, circuit
# state) and MySQL InnoDB metrics (buffer pool, row locks, DLQ count) and
# renders both panels in a single cleared-screen view.
#
# Usage:
#   ./system-monitor.sh [interval_seconds] [consumer_host]
#
# Env overrides:
#   CONSUMER_PORT  metrics API port (default: 9091)
#   DB_HOST        MySQL host (default: localhost)
#   DB_USER        MySQL user (default: chatflow)
#   DB_PASS        MySQL password (default: <DB_PASSWORD>)
#   DB_NAME        MySQL database (default: chatflow)
# ============================================================================

set -euo pipefail

INTERVAL="${1:-5}"
CONSUMER_HOST="${2:-localhost}"
CONSUMER_PORT="${CONSUMER_PORT:-9091}"
METRICS_URL="http://${CONSUMER_HOST}:${CONSUMER_PORT}"

DB_HOST="${DB_HOST:-localhost}"
DB_USER="${DB_USER:-chatflow}"
DB_PASS="${DB_PASS:-<DB_PASSWORD>}"
DB_NAME="${DB_NAME:-chatflow}"

trap 'tput cnorm 2>/dev/null || true; echo; exit 0' INT TERM

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

mysql_query() {
    mysql -u"$DB_USER" -p"$DB_PASS" -h "$DB_HOST" -N -B "$DB_NAME" -e "$1" 2>/dev/null || true
}

mysql_status() {
    mysql -u"$DB_USER" -p"$DB_PASS" -h "$DB_HOST" -N -B -e "$1" 2>/dev/null || true
}

extract_json() {
    local key="$1" json="$2"
    echo "$json" | grep -o "\"${key}\":[^,}]*" | head -1 | cut -d: -f2- | tr -d '" '
}

print_header() {
    local title="$1"
    printf '\033[1;36m%s\033[0m\n' "$title"
    printf '%s\n' "----------------------------------------------------------------------"
}

# ----------------------------------------------------------------------------
# Application panel: message-processor MetricsAPI
# ----------------------------------------------------------------------------

render_app_panel() {
    print_header "APPLICATION  (${METRICS_URL})"

    local metrics
    metrics=$(curl -s --max-time 4 "$METRICS_URL/api/metrics" 2>/dev/null || true)

    if [[ -z "$metrics" ]]; then
        echo "  (consumer unreachable)"
        echo
        return
    fi

    local consume_rate write_rate total_written buffer_depth db_active avg_ms max_ms cb_state cb_trips dlq_buf dropped
    consume_rate=$(echo  "$metrics" | grep -o '"currentConsumeRate":[0-9.]*' | cut -d: -f2)
    write_rate=$(echo    "$metrics" | grep -o '"currentWriteRate":[0-9.]*'   | cut -d: -f2)
    total_written=$(echo "$metrics" | grep -o '"messagesWritten":[0-9]*'     | head -1 | cut -d: -f2)
    buffer_depth=$(echo  "$metrics" | grep -o '"currentDepth":[0-9]*'        | cut -d: -f2)
    db_active=$(echo     "$metrics" | grep -o '"activeConnections":[0-9]*'   | cut -d: -f2)
    avg_ms=$(echo        "$metrics" | grep -o '"avgBatchWriteMs":[0-9.]*'    | cut -d: -f2)
    max_ms=$(echo        "$metrics" | grep -o '"maxBatchWriteMs":[0-9]*'     | cut -d: -f2)
    cb_state=$(echo      "$metrics" | grep -o '"state":"[A-Z_]*"'            | head -1 | cut -d'"' -f4)
    cb_trips=$(echo      "$metrics" | grep -o '"totalTrips":[0-9]*'          | cut -d: -f2)
    dropped=$(echo       "$metrics" | grep -o '"totalDropped":[0-9]*'        | cut -d: -f2)
    dlq_buf=$(echo       "$metrics" | grep -o '"dlqMessages":[0-9]*'         | cut -d: -f2)

    printf "  %-22s %s\n"    "Consume rate (msg/s):" "${consume_rate:-0}"
    printf "  %-22s %s\n"    "Write rate   (msg/s):" "${write_rate:-0}"
    printf "  %-22s %s\n"    "Total written:"        "${total_written:-0}"
    printf "  %-22s %s\n"    "Buffer depth:"         "${buffer_depth:-0}"
    printf "  %-22s %s\n"    "Buffer dropped:"       "${dropped:-0}"
    printf "  %-22s %s\n"    "DLQ messages:"         "${dlq_buf:-0}"
    printf "  %-22s active=%s avg=%sms max=%sms\n" \
        "DB writer:" "${db_active:-0}" "${avg_ms:-0}" "${max_ms:-0}"
    printf "  %-22s %s (trips=%s)\n" \
        "Circuit breaker:" "${cb_state:-?}" "${cb_trips:-0}"
    echo
}

# ----------------------------------------------------------------------------
# Database panel: MySQL InnoDB metrics
# ----------------------------------------------------------------------------

render_db_panel() {
    print_header "DATABASE  (${DB_USER}@${DB_HOST}/${DB_NAME})"

    local msg_count dlq_count
    msg_count=$(mysql_query "SELECT COUNT(*) FROM messages;")
    dlq_count=$(mysql_query "SELECT COUNT(*) FROM dead_letter_messages WHERE resolved = FALSE;")

    if [[ -z "$msg_count" ]]; then
        echo "  (mysql unreachable)"
        echo
        return
    fi

    printf "  %-32s %s\n" "Messages persisted:" "$msg_count"
    printf "  %-32s %s\n" "Dead letters (unresolved):" "${dlq_count:-0}"

    local status
    status=$(mysql_status "
        SELECT VARIABLE_NAME, VARIABLE_VALUE
          FROM performance_schema.global_status
         WHERE VARIABLE_NAME IN (
            'Innodb_buffer_pool_read_requests',
            'Innodb_buffer_pool_reads',
            'Innodb_rows_inserted',
            'Threads_connected',
            'Threads_running',
            'Innodb_row_lock_waits',
            'Innodb_row_lock_time_avg',
            'Innodb_data_reads',
            'Innodb_data_writes',
            'Slow_queries'
         );
    ")

    declare -A vals=()
    while IFS=$'\t' read -r name value; do
        [[ -n "$name" ]] && vals["$name"]="$value"
    done <<< "$status"

    printf "  %-32s %s\n" "Threads connected/running:" \
        "${vals[Threads_connected]:-0} / ${vals[Threads_running]:-0}"
    printf "  %-32s reads=%s writes=%s\n" "InnoDB I/O:" \
        "${vals[Innodb_data_reads]:-0}" "${vals[Innodb_data_writes]:-0}"
    printf "  %-32s %s\n" "InnoDB rows inserted:" \
        "${vals[Innodb_rows_inserted]:-0}"
    printf "  %-32s waits=%s avg=%sms\n" "InnoDB row locks:" \
        "${vals[Innodb_row_lock_waits]:-0}" "${vals[Innodb_row_lock_time_avg]:-0}"
    printf "  %-32s %s\n" "Slow queries:" \
        "${vals[Slow_queries]:-0}"

    local reqs="${vals[Innodb_buffer_pool_read_requests]:-0}"
    local reads="${vals[Innodb_buffer_pool_reads]:-0}"
    if [[ "$reqs" =~ ^[0-9]+$ ]] && [[ "$reqs" -gt 0 ]] && command -v bc >/dev/null 2>&1; then
        local hit_ratio
        hit_ratio=$(echo "scale=4; 1 - ($reads / $reqs)" | bc 2>/dev/null || echo "N/A")
        printf "  %-32s %s\n" "Buffer pool hit ratio:" "${hit_ratio}"
    fi
    echo
}

# ----------------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------------

tput civis 2>/dev/null || true

while true; do
    clear
    printf '\033[1;37m%s\033[0m   %s\n' "ChatFlow System Monitor" "$(date '+%Y-%m-%d %H:%M:%S')"
    printf 'refresh: %ss   metrics: %s   db: %s@%s\n\n' \
        "$INTERVAL" "$METRICS_URL" "$DB_USER" "$DB_HOST"

    render_app_panel
    render_db_panel

    printf '\033[2m(Ctrl+C to exit)\033[0m\n'
    sleep "$INTERVAL"
done
