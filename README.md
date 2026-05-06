# ChatFlow Messaging System

A horizontally-scalable WebSocket chat platform with a write-behind persistence pipeline, distributed L1+L2 caching, and a CQRS-style read path. Sustains 21k msg/s sustained writes and 10k+ RPS under a 70/30 read/write JMeter mix with zero message loss.

![Java](https://img.shields.io/badge/Java-11+-orange)
![MySQL](https://img.shields.io/badge/MySQL-8.x-blue)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-Message_Broker-FF6600)
![Redis](https://img.shields.io/badge/Redis-Cache-red)

## Architecture

```
        Clients (WebSocket)
                │
                ▼
   ┌────────────────────────────┐
   │  Application Load Balancer │  sticky sessions, :8080
   └────────────┬───────────────┘
                │
       ┌────────┴────────┐
       ▼                 ▼
  gateway-server     gateway-server      Java-WebSocket → validate → publish
       │                 │
       └────────┬────────┘
                ▼
            RabbitMQ                     topic exchange, 20 room queues
                │
                ▼
        message-processor                MessageConsumerPool (4 threads, prefetch=100)
                │                          → WriteBuffer (BlockingQueue, 500K)
                │                          → DatabaseWriter (3 threads, adaptive batch 500/2K/3K)
                │                          → BroadcastServer (WebSocket fan-out)
                │                          → MetricsAPI (REST, :9091)
       ┌────────┼────────┐
       ▼        ▼        ▼
    MySQL    Redis   subscribers         InnoDB 1GB pool / L2 cache + active invalidation
```

The platform splits the read and write paths so a slow database never starves the broadcast loop, and a Redis outage degrades to a direct-MySQL fallback rather than a hard failure.

## Modules

| Module | Responsibility |
| --- | --- |
| [`gateway-server/`](gateway-server/) | WebSocket entry point. Validates client messages, multiplexes per-room AMQP channels through `ChannelPool`, publishes to RabbitMQ via `RabbitMQPublisher`, trips a `CircuitBreaker` on broker failure. |
| [`message-processor/`](message-processor/) | RabbitMQ consumer pool → write-behind buffer → batched MySQL writes → WebSocket broadcast. Hosts `MetricsAPI` (REST), `RedisCacheAdapter` (L2), `StampedeGuard` (per-key lock), and `QueryCache` (L1). Adaptive batch sizing (500 / 2K / 3K) based on queue depth. Split HikariCP pools (writer=10, reader=20). |
| [`database/`](database/) | Squashed MySQL 8 schema in `01-init-schema.sql` (`messages`, `dead_letter_messages`, `user_message_summary`, `room_message_summary`) plus `provision-db.sh`, the env-driven IaC provisioner that installs MySQL, loads the schema, and applies production InnoDB tuning (1 GB buffer pool, batch-insert tuning, slow-query log). |
| [`deployment/`](deployment/) | EC2 deployment automation: `build-and-deploy.sh` (build + scp), `run-message-processor.sh` (start with profile flags), `verify-ec2-remote.sh` (smoke tests), and `EC2-DEPLOYMENT-GUIDE.md` (operator runbook). |
| [`load-testing/`](load-testing/) | JMeter plans (`S1-baseline.jmx`, `S2-stress.jmx`, `E1-endurance.jmx`) plus a Maven Java client (`ClientMain`, `MessageGenerator`, `MetricsAPIClient`) for custom write-only load. Runner scripts wrap JMeter invocation and result download. |
| [`monitoring/`](monitoring/) | Live-poll bash scripts: `monitor.sh` (consumer metrics + queue depth) and `monitor-db.sh` (MySQL throughput, lock waits, buffer-pool hit rate). |

## Prerequisites

- **Java 11+** (Amazon Corretto 11 used in production)
- **Maven 3.6+**
- **MySQL 8.x** with InnoDB
- **RabbitMQ 3.x / 4.x**
- **Redis 7.x** (optional — fail-open if absent)
- **Apache JMeter 5.6.3** (only for load testing)

## Build

Each module is a standalone Maven project. The shade plugin produces a self-contained jar.

```bash
mvn -f gateway-server/pom.xml clean package
mvn -f message-processor/pom.xml clean package
mvn -f load-testing/pom.xml clean package
```

Or use the deployment helper:

```bash
bash deployment/build-and-deploy.sh
```

## Run

Local development (single host, all components):

```bash
# 1. Provision the database (installs MySQL, creates user, loads schema, applies tuning)
DB_PASSWORD='your-strong-password' bash database/provision-db.sh

# Or, against an existing MySQL host, just load the schema:
mysql -u root -p chatflow < database/01-init-schema.sql

# 2. Start RabbitMQ and Redis (Docker shown for brevity)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
docker run -d --name redis    -p 6379:6379 redis:7

# 3. Start the gateway
java -jar gateway-server/target/gateway-server-1.0-SNAPSHOT.jar

# 4. Start the message processor (profile-driven)
PROFILE=O12 bash deployment/run-message-processor.sh
```

The processor accepts four optimization profiles:

| Profile | Flags                                | What's active                      |
| ------- | ------------------------------------ | ---------------------------------- |
| B0      | none                                 | Baseline — direct MySQL, no caching |
| O1      | `-Dredis.host=…`                     | L1 + L2 caching with active invalidation |
| O2      | `-Dsummary.tables=true`              | Pre-aggregated summary tables       |
| O12     | both                                 | Combined — best overall throughput  |

## Load Testing

```bash
# JMeter plan (CONSUMER_HOST = MetricsAPI host, ALB_HOST = gateway entry)
RUNID=B0-S1 CONSUMER_HOST=10.0.1.5 ALB_HOST=alb.example.com \
  bash load-testing/run-jmeter.sh load-testing/S1-baseline.jmx

# Custom Java client (write-only, used for raw throughput baselining)
java -jar load-testing/target/chatflow-client-1.0-SNAPSHOT.jar \
     --threads 32 --messages 500000
```

Three packaged scenarios:

- **S1 (baseline):** 100 threads, ~5 min, 70/30 R/W
- **S2 (stress):** 500 threads, 30 min, 70/30 R/W
- **E1 (endurance):** 50 threads, 60 min, 70/30 R/W

## Performance Highlights

| Test                         | Messages    | Throughput           | Persistence    |
| ---------------------------- | ----------- | -------------------- | -------------- |
| Custom client baseline (500K) | 500,000    | 19,687 msg/s         | 100%           |
| Custom client stress (1M)    | 1,000,000   | 21,091 msg/s         | 99.98%         |
| Custom client endurance (5×500K) | 2,500,000 | 20,521 msg/s avg | 100%           |
| JMeter S2 / O2 (30 min)      | 19,866,386  | 11,021 RPS / 32 ms avg | —            |
| JMeter E1 / O12 (60 min)     | 120,311     | 33 RPS / 1,495 ms avg | survived full hour |

> **Note on E1 vs S2:** S2 is a 30-minute throughput test at 500 threads — the system is allowed to cache aggressively and ride the L1 hit rate. E1 is a 60-minute *endurance* test at 50 threads where the working set deliberately spills past the cache so every read goes to disk and every write competes with the same row-lock surface as the writer pool. The 33 RPS figure reflects sustained MySQL row-lock and InnoDB log-flush saturation, not a typo — the headline is that O12 is the only profile that *survives the full hour* (B0 and O2 alone degrade or error past acceptable thresholds).

Core read-path queries all stay well under their targets at 1M-row scale (room messages 13 ms vs 100 ms target, user history 2 ms vs 200 ms target).

## Resilience

| Pattern             | Where                         | Behavior                                            |
| ------------------- | ----------------------------- | --------------------------------------------------- |
| Circuit breaker     | `CircuitBreaker.java` (both modules) | 5 failures → OPEN, 15 s reset, soft-evict pool on trip |
| Dead-letter queue   | `dead_letter_messages` table  | Failed writes preserved with original JSON for replay |
| Idempotent writes   | UUID PK + `INSERT IGNORE`     | At-least-once delivery, exactly-once persistence    |
| Stampede guard      | `StampedeGuard.java`          | Per-key `ReentrantLock` → at most 1 DB query per miss |
| Split connection pools | HikariCP writer (10) / reader (20) | Read path unaffected by write-side failures   |
| Fail-open Redis     | `RedisCacheAdapter.java`      | Redis down → fall through to MySQL, no 5xx          |
| Active invalidation | Redis `DEL` on batch commit   | Prevents stale-read window after write              |

## Repository Layout

```
chatflow-messaging-system/
├── gateway-server/        WebSocket entry, RabbitMQ publisher
├── message-processor/     consumer pool, write-behind, broadcast, metrics API
├── database/              schema + migrations + provisioner
├── deployment/            EC2 build/deploy/run automation + runbook
├── load-testing/          JMeter plans + custom Java client + runners
├── monitoring/            live-poll observability scripts
├── ARCHITECTURE.md        deeper component & data-flow doc
└── LICENSE
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for component-level deep dives.

## License

See [LICENSE](LICENSE).
