# ChatFlow EC2 Deployment Runbook

## Overview

ChatFlow is a distributed messaging system. This runbook covers cold-start
bring-up of the full EC2 stack, write-path validation (500K / 1M / endurance),
and the four optimization profiles that exercise the read path under JMeter
load (B0 baseline, O1 Redis cache, O2 summary tables, O12 both).

**Topology principles:**

- **MySQL is dedicated** (`chatflow-mysql`) — co-locating it with the consumer
  on a t3.micro caused OOM (Java 512MB + MySQL 400MB + OS 200MB > 1GB) and
  CPU starvation that took down the metrics API. Splitting the database off
  recovered persistence (500K/500K, 0 failures) and unblocked metrics queries.
- **Redis is dedicated** (`chatflow-redis`) — separate cache instance for the
  O1/O12 read-through cache profiles.
- **Two gateway servers**, not four — throughput scales flat past two app
  hosts on this workload, so server-3/server-4 stay stopped to save credits.

**Consumer reliability fixes baked into the current build:**

- **Dual HikariCP pools** — `writerPool` (15 conns) for batch inserts and the
  DLQ; `readerPool` (3 conns) reserved for the metrics API. Prevents HTTP 500
  on `/api/metrics` when the writer pool is exhausted or the breaker is open.
- **BatchResult tracking** — `batchInsert()` reports `attempted` vs `inserted`
  separately, so `peakWriteRate` reflects real MySQL throughput rather than
  the artificially low unique-insert count.
- **Self-healing breaker** — when it trips OPEN it soft-evicts stale Hikari
  connections so the HALF_OPEN probe runs against a fresh handle.
- **Hikari tuning** — `maxLifetime=180s`, `keepaliveTime=60s` survive idle
  gaps between endurance rounds.

## Scope

- Full bring-up of the seven active EC2 instances (server-1, server-2,
  rabbitmq, consumer, mysql, redis, client).
- Write-path tests at 500K, 1M, and 5×500K endurance.
- Optimization runs (B0, O1, O2, O12) using JMeter scenarios S1, S2, E1.
- Artifact locations and pull-to-laptop commands for archiving runs.

---

## Infrastructure (7 active EC2 instances)

```text
              EC2: Client (t3.micro)
              32 threads, 500K–1M msgs
                       |
                  ws:// (ALB DNS :8080)
                       |
         Application Load Balancer (:8080)
         Sticky sessions | Health :8081
              /                \
         Server-1            Server-2          ← two-host fleet (flat scaling)
         (:8080)             (:8080)
              \                /
             AMQP :5672 (private IP)
                       |
              EC2: RabbitMQ (t3.micro)
              chat.exchange → room.1-20
                       |
              AMQP :5672 (basicConsume)
                       |
              EC2: Consumer (t3.micro)         ← Java only, no co-located DB
              WriteBuffer → DB writers          full 1GB RAM for the JVM
              Metrics API :9091
                       |
              TCP :6379 (private IP)
                       |
              EC2: Redis (t3.micro)            ← chatflow-redis (cache)
              Used by O1 / O12 profiles
                       |
              JDBC :3306 (private IP)
                       |
              EC2: MySQL (t3.micro)            ← chatflow-mysql (dedicated)
              InnoDB, 400MB buffer pool         full 1GB RAM for the DB
              chatflow database
```

### Why this split

Co-locating MySQL with the Java consumer on one t3.micro caused:

- **OOM**: Java 512MB + MySQL 400MB + OS 200MB > 1024MB available.
- **CPU starvation**: MySQL batch INSERTs pinned the vCPU; the metrics API
  stopped responding.
- **Write loss**: 5,540 `dbFailed`, only 82 of 500K messages persisted.

After splitting MySQL off:

- **500K**: 500,000/500,000 persisted, 0 failures, all queries inside SLO.
- **1M**: 985,380 persisted, 0 DB failures, breaker never tripped.

The same pattern earlier showed +26% throughput when RabbitMQ was split off
the consumer.

---

## Step 0: Collect environment values

```bash
# SSH key
KEY=~/distributed_systems/chatflow-key.pem

# Public IPs (re-issue on every stop/start — confirm in AWS Console)
SERVER1_PUBLIC_IP=54.201.74.228
SERVER2_PUBLIC_IP=54.214.152.223
RABBITMQ_PUBLIC_IP=16.148.202.196
MYSQL_PUBLIC_IP=52.88.157.39        # chatflow-mysql
REDIS_PUBLIC_IP=35.88.141.194       # chatflow-redis
CONSUMER_PUBLIC_IP=16.145.82.67
CLIENT_PUBLIC_IP=34.219.142.67

# Private IPs (stable within VPC)
RABBITMQ_PRIVATE_IP=172.31.29.217
MYSQL_PRIVATE_IP=172.31.24.182
REDIS_PRIVATE_IP=172.31.21.68
CONSUMER_PRIVATE_IP=172.31.19.61

# ALB DNS (stable)
ALB_DNS=chatflow-alb-1805851036.us-west-2.elb.amazonaws.com
```

### Smoke test (laptop)

After the variables above match running instances:

```bash
cd deployment
chmod +x build-and-deploy.sh
./build-and-deploy.sh
```

`build-and-deploy.sh` builds, uploads, and runs the verification phase
automatically. Override any IP by exporting the same names from Step 0
before calling the script. To skip the verification phase, set
`SKIP_VERIFY=1`.

### Command host map

Use this map so each command runs on the correct host.

| Where command runs      | What you do there                                            |
| ----------------------- | ------------------------------------------------------------ |
| Laptop (local)          | Build JARs, upload files, SSH into EC2, pull artifacts back  |
| RabbitMQ EC2            | Start/verify RabbitMQ                                        |
| Server-1 / Server-2 EC2 | Start gateway-server processes                                |
| chatflow-mysql EC2      | MySQL setup, DB reset, count verification                    |
| chatflow-redis EC2      | Redis setup and cache health checks                          |
| Consumer EC2            | Start/stop message-processor, check metrics API and logs     |
| Client EC2              | Run the load-test client and JMeter harness                  |

---

## Step 1: Start the EC2 instances

AWS Console → EC2 → Instances. Start the seven active instances:

1. `chatflow-server-1`
2. `chatflow-server-2`
3. `chatflow-mysql`
4. `chatflow-redis`
5. `chatflow-rabbitmq`
6. `chatflow-consumer`
7. `chatflow-client`

Capture the new public IPs (they change on every stop/start).

### One-time: provision `chatflow-redis`

Skip this if the instance already exists.

- Launch instance → name `chatflow-redis`, AMI Amazon Linux 2023, type t3.micro
- Reuse the existing ChatFlow key pair
- Same VPC + AZ as consumer/mysql to minimize cross-AZ latency
- Auto-assign public IP for setup convenience
- Security group inbound rules:
  - SSH (22) from your laptop IP
  - Redis (6379) from the consumer security group (preferred) or VPC CIDR
- Storage: default gp3 (8GB)
- Record both public + private IP in Step 0 (`REDIS_PUBLIC_IP`, `REDIS_PRIVATE_IP`)

---

## Step 2: Verify the ALB target group

AWS Console → EC2 → Target Groups → `chatflow-servers` → Targets.

Confirm only `server-1` and `server-2` are registered. Deregister anything
else (e.g. `chatflow-mysql`, decommissioned servers) and wait for the
deregister to finish.

---

## Step 3: Start RabbitMQ

```bash
ssh -i $KEY ec2-user@${RABBITMQ_PUBLIC_IP}

sudo systemctl status rabbitmq-server
# If not running:
sudo systemctl start rabbitmq-server

# Verify the admin user exists
sudo rabbitmqctl list_users
# If 'admin' is missing:
# sudo rabbitmqctl add_user admin password123
# sudo rabbitmqctl set_user_tags admin administrator
# sudo rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"

exit
```

---

## Step 4: Start the gateway servers

The gateway-server JAR is unchanged across optimization runs.

### Server 1

```bash
ssh -i $KEY ec2-user@${SERVER1_PUBLIC_IP}

RABBITMQ_HOST=172.31.29.217 \
RABBITMQ_USER=admin \
RABBITMQ_PASS=password123 \
CHANNEL_POOL_SIZE=50 \
SERVER_ID=server-1 \
nohup java -Xmx512m -jar ~/gateway-server-1.0-SNAPSHOT.jar > ~/server.log 2>&1 &

sleep 3 && tail -5 ~/server.log
# Expected: HTTP health endpoint started on port 8081
```

### Server 2

Same command, change `SERVER_ID=server-2`.

### Verify health and ALB

```bash
# After ~60s for ALB health checks:
curl http://${ALB_DNS}:8081/health
# Expected: healthy response
```

If ALB :8081 is unreachable from the laptop (the LB may only expose 8080 for
WebSocket traffic), hit the targets directly:

```bash
curl http://${SERVER1_PUBLIC_IP}:8081/health
curl http://${SERVER2_PUBLIC_IP}:8081/health
```

---

## Step 5: Start MySQL on `chatflow-mysql`

```bash
ssh -i $KEY ec2-user@${MYSQL_PUBLIC_IP}

sudo systemctl start mysqld
sudo systemctl status mysqld

mysql -u chatflow -p'ChatFlow@2026' chatflow -e "SHOW TABLES;"
# Expected: messages, dead_letter_messages

sudo grep bind-address /etc/my.cnf.d/chatflow-tuning.cnf
# Expected: bind-address = 0.0.0.0
```

### First-time install (only if `chatflow-mysql` is fresh)

```bash
sudo dnf install -y https://dev.mysql.com/get/mysql80-community-release-el9-5.noarch.rpm
sudo dnf install -y mysql-community-server mysql-community-client

sudo systemctl start mysqld
sudo systemctl enable mysqld

TEMP_PASS=$(sudo grep 'temporary password' /var/log/mysqld.log | awk '{print $NF}')

mysql --connect-expired-password -u root -p"$TEMP_PASS" <<'SQL'
ALTER USER 'root'@'localhost' IDENTIFIED BY 'ChatFlow@2026';
CREATE USER 'chatflow'@'localhost' IDENTIFIED BY 'ChatFlow@2026';
CREATE USER 'chatflow'@'%' IDENTIFIED BY 'ChatFlow@2026';
CREATE DATABASE chatflow;
GRANT ALL PRIVILEGES ON chatflow.* TO 'chatflow'@'localhost';
GRANT ALL PRIVILEGES ON chatflow.* TO 'chatflow'@'%';
FLUSH PRIVILEGES;
SQL

mysql -u chatflow -p'ChatFlow@2026' chatflow <<'SCHEMA'
CREATE TABLE IF NOT EXISTS messages (
    message_id  VARCHAR(36) NOT NULL,
    room_id     INT         NOT NULL,
    user_id     INT         NOT NULL,
    username    VARCHAR(20) NOT NULL,
    message     VARCHAR(500) NOT NULL,
    message_type ENUM('TEXT', 'JOIN', 'LEAVE') NOT NULL,
    timestamp   DATETIME(3) NOT NULL,
    server_id   VARCHAR(50) DEFAULT NULL,
    client_ip   VARCHAR(45) DEFAULT NULL,
    created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (message_id),
    INDEX idx_room_timestamp (room_id, timestamp, message_id, user_id, username, message_type),
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_timestamp_user (timestamp, user_id),
    INDEX idx_user_id (user_id),
    INDEX idx_user_room_activity (user_id, room_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC;

CREATE TABLE IF NOT EXISTS dead_letter_messages (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id  VARCHAR(36) NOT NULL,
    message_json TEXT NOT NULL,
    error_reason VARCHAR(500) DEFAULT NULL,
    retry_count INT DEFAULT 0,
    created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    last_retry  DATETIME(3) DEFAULT NULL,
    resolved    BOOLEAN DEFAULT FALSE,
    INDEX idx_dl_unresolved (resolved, last_retry),
    INDEX idx_dl_message_id (message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SCHEMA

sudo tee /etc/my.cnf.d/chatflow-tuning.cnf > /dev/null <<'EOF'
[mysqld]
innodb_buffer_pool_size = 400M
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT
innodb_log_buffer_size = 32M
innodb_write_io_threads = 4
innodb_read_io_threads = 4
innodb_autoinc_lock_mode = 2
bulk_insert_buffer_size = 32M
max_connections = 50
thread_cache_size = 8
skip-log-bin
bind-address = 0.0.0.0
EOF

sudo systemctl restart mysqld
```

### Security group

Confirm `launch-wizard-2` allows MySQL (3306) inbound from the VPC:

- AWS Console → EC2 → Security Groups → `launch-wizard-2` → Edit inbound rules
- Type=MySQL/Aurora, Port=3306, Source=172.31.0.0/16

---

## Step 5.5: Provision Redis on `chatflow-redis` (required for O1/O12)

```bash
ssh -i $KEY ec2-user@${REDIS_PUBLIC_IP}

sudo dnf install -y redis6 || sudo dnf install -y redis

REDIS_CONF="/etc/redis/redis.conf"
[ -f /etc/redis6.conf ] && REDIS_CONF="/etc/redis6.conf"
[ -f /etc/redis.conf ]  && REDIS_CONF="/etc/redis.conf"

sudo sed -i 's/^bind .*/bind 0.0.0.0/'              "$REDIS_CONF"
sudo sed -i 's/^protected-mode .*/protected-mode no/' "$REDIS_CONF"
grep -q '^bind '            "$REDIS_CONF" || echo 'bind 0.0.0.0'        | sudo tee -a "$REDIS_CONF"
grep -q '^protected-mode '  "$REDIS_CONF" || echo 'protected-mode no'   | sudo tee -a "$REDIS_CONF"

sudo systemctl enable  redis || sudo systemctl enable  redis6
sudo systemctl restart redis || sudo systemctl restart redis6

redis6-cli -h 127.0.0.1 -p 6379 PING
# Expected: PONG
```

### Redis security group

- Type=Custom TCP, Port=6379, Source=consumer security group (preferred)

### Verify from the consumer

```bash
ssh -i $KEY ec2-user@${CONSUMER_PUBLIC_IP}
sudo dnf install -y redis6 || sudo dnf install -y redis
redis6-cli -h ${REDIS_PRIVATE_IP} -p 6379 PING
# Expected: PONG
```

---

## Step 6: Build and upload JARs (laptop)

```bash
# From the repo root
deployment/build-and-deploy.sh
# Builds gateway-server, message-processor, client-part2, uploads JARs and
# the schema files, then runs the verification phase.
```

Manual upload (only if you need to push a single artifact without rebuilding):

```bash
scp -i $KEY message-processor/target/message-processor-1.0-SNAPSHOT.jar \
  ec2-user@${CONSUMER_PUBLIC_IP}:~/message-processor.jar

scp -i $KEY client-part2/target/chatflow-client-1.0-SNAPSHOT.jar \
  ec2-user@${CLIENT_PUBLIC_IP}:~/client-v3.jar

scp -i $KEY -r load-testing \
  ec2-user@${CLIENT_PUBLIC_IP}:~/load-tests

scp -i $KEY monitoring/system-monitor.sh \
  ec2-user@${CONSUMER_PUBLIC_IP}:~/system-monitor.sh
```

### Verify the summary tables (required for O2 / O12)

The summary tables ship in the squashed schema applied by `provision-db.sh`,
so no separate migration is needed. Sanity-check before O2 / O12 runs:

```bash
ssh -i $KEY ec2-user@${MYSQL_PUBLIC_IP}

mysql -u chatflow -p"$DB_PASSWORD" chatflow -e "SHOW TABLES LIKE 'user_message_summary';"
mysql -u chatflow -p"$DB_PASSWORD" chatflow -e "SHOW TABLES LIKE 'room_message_summary';"

# If either is missing, re-run the provisioner or load the schema directly:
mysql -u chatflow -p"$DB_PASSWORD" chatflow < ~/01-init-schema.sql
```

---

## Step 7: Start the consumer (`message-processor`)

```bash
ssh -i $KEY ec2-user@${CONSUMER_PUBLIC_IP}

# Make sure no local MySQL is running on this host
sudo systemctl stop    mysqld 2>/dev/null
sudo systemctl disable mysqld 2>/dev/null

# Confirm the consumer can reach the dedicated MySQL
mysql -u chatflow -p'ChatFlow@2026' -h 172.31.24.182 chatflow -e "SELECT 1;"

# Pick the optimization profile for this run
# B0  (baseline): leave EXTRA_JAVA_FLAGS empty
EXTRA_JAVA_FLAGS=""
# O1  (Redis):    EXTRA_JAVA_FLAGS="-Dredis.host=${REDIS_PRIVATE_IP} -Dredis.port=6379"
# O2  (Summary):  EXTRA_JAVA_FLAGS="-Dsummary.tables=true"
# O12 (Both):     EXTRA_JAVA_FLAGS="-Dredis.host=${REDIS_PRIVATE_IP} -Dredis.port=6379 -Dsummary.tables=true"

RABBITMQ_HOST=172.31.29.217 \
RABBITMQ_PORT=5672 \
RABBITMQ_USER=admin \
RABBITMQ_PASS=password123 \
CONSUMER_THREADS=4 \
PREFETCH_COUNT=100 \
DB_HOST=172.31.24.182 \
DB_PORT=3306 \
DB_NAME=chatflow \
DB_USER=chatflow \
DB_PASS="ChatFlow@2026" \
DB_POOL_SIZE=15 \
WRITE_BUFFER_CAPACITY=1500000 \
WRITE_BATCH_SIZE=5000 \
WRITER_THREADS=5 \
nohup java -Xmx512m -Xms256m -XX:+UseG1GC ${EXTRA_JAVA_FLAGS} \
  -jar ~/message-processor.jar > ~/message-processor.log 2>&1 &

echo "Started (PID: $!)"
sleep 5
tail -20 ~/message-processor.log
```

Expected startup log:

```text
DatabaseManager initialized: writerPool=15, readerPool=3, host=172.31.24.182:3306/chatflow
WriteBuffer initialized: capacity=1500000, batchSize=5000
DatabaseWriter started: threads=5, flushInterval=500ms, batchSize=5000
StatsAggregator started: interval=5s
Metrics API started on port 9091
=== message-processor ready and listening ===
```

Verify:

```bash
free -m
# Expected: ~550MB+ free (no MySQL on this host)

curl -s http://127.0.0.1:9091/api/health | python3 -m json.tool
# circuitBreaker: CLOSED, dbConnections.total > 0

grep -E 'Redis cache (enabled|disabled)|summary.tables|DatabaseManager initialized' ~/message-processor.log
```

---

## Step 7.5: Batch-size calibration (run once)

Find the optimal `WRITE_BATCH_SIZE` for the current topology. The harness
runs 100K messages per batch size and records peak write rate. Drive it
from the laptop (so SSH sessions are not the targets of `pkill -f`):

```bash
KEY=~/distributed_systems/chatflow-key.pem \
MYSQL_PUBLIC_IP=$MYSQL_PUBLIC_IP \
CONSUMER_PUBLIC_IP=$CONSUMER_PUBLIC_IP \
CLIENT_PUBLIC_IP=$CLIENT_PUBLIC_IP \
ALB_HOST=$ALB_DNS \
load-testing/run-benchmarks.sh --batch-experiment
```

Each batch produces a `batch_result_<size>.json` plus a console summary
(`peakWriteRate`, `avgBatchMs`, `maxBatchMs`). Capture the table.

### Pick the winner

Choose the batch with the highest `peakWriteRate`. If two are close, prefer
the lower `maxBatchMs` for steadier tail latency.

In one observed 100K calibration: **500** had the best peak (~11.9k msg/s).
**1000** had the lowest average batch time but lower peak. **5000** kept
average batch time reasonable but `maxBatchMs` spiked (~1.5s), making it a
weaker default unless explicitly optimizing for large batches.

### Restart the consumer with the winning batch size

```bash
ssh -i $KEY ec2-user@${CONSUMER_PUBLIC_IP}
pkill -f message-processor.jar || true
sleep 2
# Re-run the Step 7 startup with WRITE_BATCH_SIZE=<winner> and the same
# EXTRA_JAVA_FLAGS you intend to test (e.g. -Dredis.host=… for O1/O12).
```

---

## Step 8: Test 1 — baseline 500K

### Terminal A — consumer EC2 (start *before* the test)

```bash
ssh -i $KEY ec2-user@${CONSUMER_PUBLIC_IP}
~/system-monitor.sh 5 > ~/db-monitor-test1-$(date +%Y%m%d-%H%M%S).log 2>&1 &
MONITOR_PID=$!
echo "Monitor PID: $MONITOR_PID"
```

### Terminal B — chatflow-mysql

```bash
ssh -i $KEY ec2-user@${MYSQL_PUBLIC_IP}
mysql -u chatflow -p'ChatFlow@2026' chatflow -e "TRUNCATE TABLE messages; TRUNCATE TABLE dead_letter_messages;"
exit
```

### Terminal C — client EC2

```bash
ssh -i $KEY ec2-user@${CLIENT_PUBLIC_IP}

java -Xmx512m -jar ~/client-v3.jar \
  "ws://${ALB_DNS}:8080/chat/" \
  "http://${CONSUMER_PRIVATE_IP}:9091" \
  500000
```

### Terminal A — stop the monitor

```bash
kill $MONITOR_PID
cat ~/db-monitor-test1-*.log
```

### Verify (chatflow-mysql)

```bash
mysql -u chatflow -p'ChatFlow@2026' chatflow -N -e "SELECT COUNT(*) FROM messages;"
# Expected: 500000
```

Expected results:

- `totalMessagesInDB: 500000`
- `messagesAttempted: 500000`, `messagesInserted: 500000` (or near-equal on a fresh DB)
- `dbFailed: 0`, `dlqMessages: 0`
- `circuitBreaker: CLOSED`
- Q1 < 100ms, Q2 < 200ms, Q3 < 500ms, Q4 < 50ms
- `/api/metrics` and `/api/all` return 200 (dual-pool fix)

---

## Step 9: Test 2 — stress 1M

### Terminal A — restart consumer + start monitor (1M run)

```bash
ssh -i $KEY ec2-user@${CONSUMER_PUBLIC_IP}

pkill -f message-processor.jar && sleep 3
# Re-run the Step 7 startup. Wait for: "message-processor ready and listening"

~/system-monitor.sh 5 > ~/db-monitor-test2-$(date +%Y%m%d-%H%M%S).log 2>&1 &
MONITOR_PID=$!
```

### Terminal B — chatflow-mysql: reset (1M run)

```bash
ssh -i $KEY ec2-user@${MYSQL_PUBLIC_IP}
mysql -u chatflow -p'ChatFlow@2026' chatflow -e "TRUNCATE TABLE messages; TRUNCATE TABLE dead_letter_messages;"
exit
```

### Terminal C — client EC2 (1M run)

```bash
ssh -i $KEY ec2-user@${CLIENT_PUBLIC_IP}

java -Xmx512m -jar ~/client-v3.jar \
  "ws://${ALB_DNS}:8080/chat/" \
  "http://${CONSUMER_PRIVATE_IP}:9091" \
  1000000
```

### Terminal A — stop the monitor (1M run)

```bash
kill $MONITOR_PID
```

**Note on client-side failures:** the 1M run typically reports ~17K–18K
WebSocket failures (`Failed: 17976`). These are ALB connection rejections at
peak load — the server-side write pipeline and MySQL persistence are
unaffected (`dbFailed: 0`). Document this as the observed bottleneck: the
ALB / WebSocket connection capacity saturates before the persistence layer.
Confirm `dbFailed: 0` and `circuitBreaker: CLOSED` in the consumer log.

---

## Step 10: Test 3 — endurance (5 rounds × 500K)

Restart the consumer (zeroes all metrics counters) and truncate the DB before
round 1 so duplicates do not skew the inserted count.

### Terminal A — consumer EC2: restart + monitor (endurance)

```bash
ssh -i $KEY ec2-user@${CONSUMER_PUBLIC_IP}

pkill -f message-processor.jar && sleep 3
# Re-run the Step 7 startup (WRITE_BUFFER_CAPACITY=1500000 is critical).
# Wait for: "message-processor ready and listening"

~/system-monitor.sh 10 > ~/db-monitor-test3-$(date +%Y%m%d-%H%M%S).log 2>&1 &
MONITOR_PID=$!
echo "Monitor PID: $MONITOR_PID  (keep this terminal open)"
```

### Terminal B — chatflow-mysql: reset (endurance)

```bash
ssh -i $KEY ec2-user@${MYSQL_PUBLIC_IP}
mysql -u chatflow -p'ChatFlow@2026' chatflow -e "TRUNCATE TABLE messages; TRUNCATE TABLE dead_letter_messages;"
exit
```

### Terminal C — client EC2: 5 rounds

```bash
ssh -i $KEY ec2-user@${CLIENT_PUBLIC_IP}

ALB="ws://chatflow-alb-1805851036.us-west-2.elb.amazonaws.com:8080/chat/"
CONSUMER="http://172.31.19.61:9091"

for ROUND in 1 2 3 4 5; do
  echo ""
  echo "=========================================="
  echo "=== Endurance round $ROUND of 5 — $(date) ==="
  echo "=========================================="

  java -Xmx512m -jar ~/client-v3.jar "$ALB" "$CONSUMER" 500000
  echo "Round $ROUND finished at: $(date)"

  echo "--- Metrics after round $ROUND ---"
  curl -s "$CONSUMER/api/metrics" | python3 -c "
import sys,json; d=json.load(sys.stdin)
t=d['throughput']; to=d['totals']; cb=d['circuitBreaker']
print(f\"  circuitBreaker={cb['state']}  trips={cb['totalTrips']}\")
print(f\"  consumed={to['messagesConsumed']}  inserted={to['messagesInserted']}  failed={to['dbFailed']}  dlq={to['dlqMessages']}\")
print(f\"  peakWrite={t['peakWriteRate']} msg/s  peakConsume={t['peakConsumeRate']} msg/s\")"

  if [ "$ROUND" -lt 5 ]; then
    echo "Cooldown 30s before round $((ROUND+1))..."
    sleep 30
  fi
done

echo ""
echo "=== Endurance complete ==="
echo "Final DB count:"
ssh -i $KEY ec2-user@${MYSQL_PUBLIC_IP} \
  "mysql -u chatflow -p'ChatFlow@2026' chatflow -N -e 'SELECT COUNT(*) FROM messages;'"
```

### Terminal A — stop the monitor after round 5

```bash
kill $MONITOR_PID
```

**Expected:** the breaker stays CLOSED through all five rounds with the
Hikari fixes (`maxLifetime=180s`, `keepalive=60s`). If it trips OPEN in
rounds 3–5 it should self-heal within `CB_RESET_TIMEOUT_MS=15s` via the
soft-evict callback — record the trip and recovery as correct
fault-tolerance behavior.

---

## Step 10.5: Install JMeter on the client EC2 (one-time)

```bash
ssh -i $KEY ec2-user@${CLIENT_PUBLIC_IP}

sudo dnf install -y java-17-amazon-corretto-headless wget tar

if [ ! -d "$HOME/apache-jmeter-5.6.3" ]; then
  cd ~
  wget https://downloads.apache.org/jmeter/binaries/apache-jmeter-5.6.3.tgz
  tar -xzf apache-jmeter-5.6.3.tgz
fi

cd ~/load-tests
chmod +x run-benchmarks.sh
mkdir -p artifacts
```

## Step 10.6: Run the optimization matrix (B0, O1, O2, O12)

Each profile runs S1, S2, and E1.

| Profile | Consumer `EXTRA_JAVA_FLAGS`                                                  |
| ------- | ---------------------------------------------------------------------------- |
| B0      | `""`                                                                         |
| O1      | `"-Dredis.host=${REDIS_PRIVATE_IP} -Dredis.port=6379"`                       |
| O2      | `"-Dsummary.tables=true"`                                                    |
| O12     | `"-Dredis.host=${REDIS_PRIVATE_IP} -Dredis.port=6379 -Dsummary.tables=true"` |

For each profile:

### A) Restart the consumer with the profile flags

Use the Step 7 startup, setting `EXTRA_JAVA_FLAGS` to the profile value.

### B) Reset DB state (chatflow-mysql)

```bash
ssh -i $KEY ec2-user@${MYSQL_PUBLIC_IP}
mysql -u chatflow -p'ChatFlow@2026' chatflow -e "TRUNCATE TABLE messages; TRUNCATE TABLE dead_letter_messages;"
mysql -u chatflow -p'ChatFlow@2026' chatflow -e "TRUNCATE TABLE user_message_summary;" 2>/dev/null || true
mysql -u chatflow -p'ChatFlow@2026' chatflow -e "TRUNCATE TABLE room_message_summary;" 2>/dev/null || true
```

### C) Run S1 and S2 with concurrent write pressure (client EC2)

```bash
ssh -i $KEY ec2-user@${CLIENT_PUBLIC_IP}
cd ~/load-tests

PROFILE=B0  # B0 / O1 / O2 / O12
ALB_HOST=chatflow-alb-1805851036.us-west-2.elb.amazonaws.com
CONSUMER_HOST=172.31.19.61

# S1 (5 min)
java -Xmx512m -jar ~/client-v3.jar "ws://${ALB_HOST}:8080/chat/" "http://${CONSUMER_HOST}:9091" 120000 \
  > artifacts/${PROFILE}-S1-writes.log 2>&1 &
WRITER_PID=$!
RUNID=${PROFILE}-S1 CONSUMER_HOST=$CONSUMER_HOST ALB_HOST=$ALB_HOST \
  ./run-benchmarks.sh --jmeter S1-baseline.jmx
wait $WRITER_PID || true

# S2 (30 min)
java -Xmx512m -jar ~/client-v3.jar "ws://${ALB_HOST}:8080/chat/" "http://${CONSUMER_HOST}:9091" 700000 \
  > artifacts/${PROFILE}-S2-writes.log 2>&1 &
WRITER_PID=$!
RUNID=${PROFILE}-S2 CONSUMER_HOST=$CONSUMER_HOST ALB_HOST=$ALB_HOST \
  ./run-benchmarks.sh --jmeter S2-stress.jmx
wait $WRITER_PID || true
```

### D) E1 endurance — two client terminals

Terminal C1 (client writes):

```bash
ssh -i $KEY ec2-user@${CLIENT_PUBLIC_IP}
cd ~/load-tests

PROFILE=B0
ALB_HOST=chatflow-alb-1805851036.us-west-2.elb.amazonaws.com
CONSUMER_HOST=172.31.19.61

for ROUND in 1 2 3 4 5; do
  echo "Round $ROUND start: $(date)" | tee -a artifacts/${PROFILE}-E1-client.log
  java -Xmx512m -jar ~/client-v3.jar "ws://${ALB_HOST}:8080/chat/" "http://${CONSUMER_HOST}:9091" 500000 \
    | tee -a artifacts/${PROFILE}-E1-client.log
  [ "$ROUND" -lt 5 ] && sleep 30
done
```

Terminal C2 (JMeter polling):

```bash
ssh -i $KEY ec2-user@${CLIENT_PUBLIC_IP}
cd ~/load-tests

PROFILE=B0
CONSUMER_HOST=172.31.19.61
ALB_HOST=chatflow-alb-1805851036.us-west-2.elb.amazonaws.com

RUNID=${PROFILE}-E1 CONSUMER_HOST=$CONSUMER_HOST ALB_HOST=$ALB_HOST \
  ./run-benchmarks.sh --jmeter E1-endurance.jmx
```

Repeat the entire flow for `PROFILE=B0`, `O1`, `O2`, `O12`.

## Step 10.7: Where results live and how to pull them

### On EC2

| Artifact                       | Host               | Path                                                                                       |
| ------------------------------ | ------------------ | ------------------------------------------------------------------------------------------ |
| JMeter raw results             | client EC2         | `~/load-tests/artifacts/<RUNID>.jtl`                                                       |
| JMeter HTML report             | client EC2         | `~/load-tests/artifacts/report-<RUNID>/index.html`                                         |
| JMeter metrics snapshots       | client EC2         | `~/load-tests/artifacts/<RUNID>-metrics-start.json` / `-mid.json` / `-end.json`            |
| JMeter runtime log             | client EC2         | `~/load-tests/artifacts/<RUNID>-jmeter.log`                                                |
| Write-pressure client logs     | client EC2         | `~/load-tests/artifacts/<PROFILE>-S1-writes.log`, `...-S2-writes.log`, `...-E1-client.log` |
| Endurance outputs              | client EC2         | `~/results-test3-endurance-*/`                                                             |
| Consumer service log           | consumer EC2       | `~/message-processor.log`                                                                  |
| DB monitor logs                | consumer EC2       | `~/db-monitor-test*.log`                                                                   |
| Redis service health           | chatflow-redis EC2 | `sudo systemctl status redis` and `redis-cli -h 127.0.0.1 -p 6379 PING`                    |
| Persistence truth source       | chatflow-mysql EC2 | `SELECT COUNT(*) FROM messages;`                                                           |

### Pull to laptop

Run from the laptop:

```bash
HANDOFF_DIR="results/handoff-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$HANDOFF_DIR"

# JMeter artifacts + endurance result folders
scp -i $KEY -r ec2-user@${CLIENT_PUBLIC_IP}:~/load-tests/artifacts "$HANDOFF_DIR/client-artifacts"
scp -i $KEY -r ec2-user@${CLIENT_PUBLIC_IP}:~/results-test*       "$HANDOFF_DIR/" 2>/dev/null || true

# Consumer logs
scp -i $KEY ec2-user@${CONSUMER_PUBLIC_IP}:~/message-processor.log "$HANDOFF_DIR/"
scp -i $KEY ec2-user@${CONSUMER_PUBLIC_IP}:~/db-monitor-test*.log  "$HANDOFF_DIR/" 2>/dev/null || true

# MySQL snapshot
ssh -i $KEY ec2-user@${MYSQL_PUBLIC_IP} "
  mysql -u chatflow -p'ChatFlow@2026' chatflow -e 'SELECT COUNT(*) AS final_messages FROM messages;' > ~/final-db-count.txt
"
scp -i $KEY ec2-user@${MYSQL_PUBLIC_IP}:~/final-db-count.txt "$HANDOFF_DIR/"

# Redis snapshot
ssh -i $KEY ec2-user@${REDIS_PUBLIC_IP} "
  (sudo systemctl status redis || sudo systemctl status redis6) > ~/redis-status.txt
"
scp -i $KEY ec2-user@${REDIS_PUBLIC_IP}:~/redis-status.txt "$HANDOFF_DIR/"

echo "Handoff bundle: $HANDOFF_DIR"
```

You can also drive the pull through the consolidated harness:

```bash
load-testing/run-benchmarks.sh --download "$CLIENT_PUBLIC_IP"
```

---

## Step 11: STOP ALL INSTANCES

**Critical — avoid AWS charges.**

AWS Console → EC2 → select all 7 running instances → Instance state → Stop instance.

---

## Troubleshooting

### Consumer cannot connect to MySQL on chatflow-mysql

```bash
mysql -u chatflow -p'ChatFlow@2026' -h 172.31.24.182 chatflow -e "SELECT 1;"

# If this fails:
# - SG must allow port 3306 from 172.31.0.0/16
# - chatflow-mysql must have bind-address = 0.0.0.0
# - 'chatflow'@'%' user must exist on chatflow-mysql
```

If MySQL returns **ERROR 1129** (`Host '…' is blocked because of many
connection errors`), on chatflow-mysql:

```bash
sudo mysqladmin flush-hosts -u root -p
# or from mysql: FLUSH HOSTS;
```

Then restart the consumer. This clears MySQL's host cache after repeated
failed handshakes (typical after a bad SG or bind-address misconfiguration).

### Consumer cannot connect to Redis on chatflow-redis

```bash
# On Redis EC2
sudo systemctl status redis || sudo systemctl status redis6
redis-cli -h 127.0.0.1 -p 6379 PING

# On consumer EC2
redis-cli -h ${REDIS_PRIVATE_IP} -p 6379 PING

# Confirm the consumer started with Redis enabled
grep -i 'Redis cache enabled' ~/message-processor.log
```

If PING fails from the consumer, check the Redis SG for port 6379 and
confirm Redis is bound to `0.0.0.0` on chatflow-redis.

### Circuit breaker trips during endurance

```bash
tail -50 ~/message-processor.log | grep -i "error\|fail\|circuit"

# The breaker trips after 5 consecutive DB failures.
# Usually caused by hitting MySQL max_connections or a connection timeout.
# Mitigation: restart the consumer between rounds, or raise max_connections.
```

### Metrics API returns HTTP 500

With the dual-pool fix, `/api/metrics` and the query endpoints use a
dedicated reader pool (3 connections, isolated from the writer pool). HTTP
500 should not occur even with the breaker OPEN or the writer pool
exhausted.

If 500 still occurs:

```bash
grep 'writerPool\|readerPool' ~/message-processor.log
# Must show: DatabaseManager initialized: writerPool=15, readerPool=3
# If you only see 'pool=' the deployed JAR is stale — rebuild and redeploy.

mysql -u chatflow -p'ChatFlow@2026' -h 172.31.24.182 chatflow -e 'SELECT 1;'
# If this fails, the reader pool also cannot connect — real connectivity issue.
```

### Reset DB between tests

```bash
mysql -u chatflow -p'ChatFlow@2026' chatflow \
  -e "TRUNCATE TABLE messages; TRUNCATE TABLE dead_letter_messages;"
```

### Memory check on consumer

```bash
free -m
# Expected: 500–600MB free (Java only, no DB)
```

---

## Configuration summary

### Consumer (EC2 t3.micro — Java only)

| Parameter        | Value         | Rationale                                             |
| ---------------- | ------------- | ----------------------------------------------------- |
| Java heap        | `-Xmx512m`    | Full 1GB instance, no MySQL competing                 |
| Consumer threads | 4             | One channel per thread, round-robin rooms             |
| Prefetch         | 100           | Messages buffered per consumer channel                |
| Write buffer     | 1,500,000     | Must cover the 1M test without blocking the consumer  |
| Batch size       | 5,000         | Optimal from batch-size calibration                   |
| Writer threads   | 5             | Parallel batch INSERTs to the remote MySQL            |
| DB writer pool   | 15            | 5 writers × 3 connections each + headroom             |
| DB reader pool   | 3 (dedicated) | Metrics API reads, isolated from writer contention    |

### MySQL (EC2 t3.micro — chatflow-mysql, dedicated)

| Parameter               | Value    | Rationale                                   |
| ----------------------- | -------- | ------------------------------------------- |
| Buffer pool             | 400MB    | ~40% of the 1GB dedicated RAM               |
| flush_log_at_trx_commit | 2        | Flush every 1s (throughput over durability) |
| flush_method            | O_DIRECT | Bypass OS cache                             |
| max_connections         | 50       | Consumer pool + queries                     |
| bind-address            | 0.0.0.0  | Accept remote connections from the consumer |
| skip-log-bin            | enabled  | No replication needed                       |

### Redis (chatflow-redis t3.micro — used by O1 / O12)

| Parameter      | Value                 | Rationale                                                     |
| -------------- | --------------------- | ------------------------------------------------------------- |
| redis host     | `${REDIS_PRIVATE_IP}` | Dedicated cache instance                                      |
| redis port     | 6379                  | Standard Redis port                                           |
| bind           | 0.0.0.0               | Reachable from the consumer over the private VPC              |
| protected-mode | no                    | Required for private remote access without auth in this build |
| startup mode   | systemd-managed       | Auto-restart on instance reboot                               |

### Instance roles (7 active)

| Instance          | Role                  | Notes                                |
| ----------------- | --------------------- | ------------------------------------ |
| chatflow-server-1 | gateway-server (ALB)  | Standard gateway host                |
| chatflow-server-2 | gateway-server (ALB)  | Standard gateway host                |
| chatflow-mysql    | **MySQL**             | Dedicated database — not in ALB      |
| chatflow-redis    | Redis cache           | Dedicated cache (O1 / O12)           |
| chatflow-rabbitmq | RabbitMQ broker       | Standard broker host                 |
| chatflow-consumer | message-processor     | Java only, no co-located MySQL       |
| chatflow-client   | Load-test driver      | Runs client jar + JMeter harness     |
