package com.chatflow.processor;

/**
 * Entry point for the ChatFlow message processor with database persistence.
 *
 * Architecture:
 *   RabbitMQ → MessageConsumerPool → WriteBuffer (blocking put) → DatabaseWriter → MySQL
 *                    ↓                                                    ↓
 *              RoomManager/Broadcast                              StatsAggregator
 *                                                                       ↓
 *                                                                  Metrics API
 *
 * Key design: WriteBuffer.put() is BLOCKING — consumer naturally throttles
 * to DB write speed. Zero message loss guaranteed.
 *
 * Reliability features:
 *   - Circuit breaker triggers HikariCP soft-evict on OPEN → HALF_OPEN transition,
 *     ensuring fresh connections for recovery test (fixes endurance test freeze)
 *   - HikariCP maxLifetime=180s, keepaliveTime=60s prevents stale connections
 *   - BatchResult tracks attempted vs inserted separately (fixes misleading peakWriteRate)
 *   - All JSON built with Gson (fixes special character handling)
 *
 * Configuration via environment variables:
 *   RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
 *   CONSUMER_THREADS (default: 4), PREFETCH_COUNT (default: 100)
 *   BROADCAST_WS_PORT (default: 9090), METRICS_API_PORT (default: 9091)
 *   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, DB_POOL_SIZE (default: 15)
 *   WRITE_BUFFER_CAPACITY (default: 1500000), WRITE_BATCH_SIZE (default: 5000)
 *   WRITER_THREADS (default: 5), FLUSH_INTERVAL_MS (default: 500)
 *   CB_FAILURE_THRESHOLD (default: 5), CB_RESET_TIMEOUT_MS (default: 15000)
 */
public class ConsumerMain {

    public static void main(String[] args) throws Exception {

        // ============ CONFIGURATION ============
        String rabbitHost = getEnv("RABBITMQ_HOST", "localhost");
        int rabbitPort = Integer.parseInt(getEnv("RABBITMQ_PORT", "5672"));
        String rabbitUser = getEnv("RABBITMQ_USER", "guest");
        String rabbitPass = getEnv("RABBITMQ_PASS", "guest");
        int consumerThreads = Integer.parseInt(getEnv("CONSUMER_THREADS", "4"));
        int prefetchCount = Integer.parseInt(getEnv("PREFETCH_COUNT", "100"));
        int broadcastWsPort = Integer.parseInt(getEnv("BROADCAST_WS_PORT", "9090"));
        int metricsApiPort = Integer.parseInt(getEnv("METRICS_API_PORT", "9091"));

        String dbHost = getEnv("DB_HOST", "localhost");
        int dbPort = Integer.parseInt(getEnv("DB_PORT", "3306"));
        String dbName = getEnv("DB_NAME", "chatflow");
        String dbUser = getEnv("DB_USER", "chatflow");
        String dbPass = getEnv("DB_PASS", "ChatFlow@2026");
        int dbPoolSize = Integer.parseInt(getEnv("DB_POOL_SIZE", "15"));

        int writeBufferCapacity = Integer.parseInt(getEnv("WRITE_BUFFER_CAPACITY", "1500000"));
        int writeBatchSize = Integer.parseInt(getEnv("WRITE_BATCH_SIZE", "5000"));
        int writerThreads = Integer.parseInt(getEnv("WRITER_THREADS", "5"));
        long flushIntervalMs = Long.parseLong(getEnv("FLUSH_INTERVAL_MS", "500"));

        int cbFailureThreshold = Integer.parseInt(getEnv("CB_FAILURE_THRESHOLD", "5"));
        long cbResetTimeoutMs = Long.parseLong(getEnv("CB_RESET_TIMEOUT_MS", "15000"));

        // ============ PRINT CONFIG ============
        System.out.println("=== ChatFlow Message Processor ===");
        System.out.println("RabbitMQ: " + rabbitHost + ":" + rabbitPort);
        System.out.println("Consumer Threads: " + consumerThreads + ", Prefetch: " + prefetchCount);
        System.out.println("Database: " + dbHost + ":" + dbPort + "/" + dbName + " (pool=" + dbPoolSize + ")");
        System.out.println("Write Buffer: capacity=" + writeBufferCapacity + ", batchSize=" + writeBatchSize);
        System.out.println("Writer Threads: " + writerThreads + ", Flush Interval: " + flushIntervalMs + "ms");
        System.out.println("Circuit Breaker: threshold=" + cbFailureThreshold + ", timeout=" + cbResetTimeoutMs + "ms");

        // ============ INITIALIZE COMPONENTS ============

        // 1. Database Manager (connection pool + queries)
        DatabaseManager dbManager = new DatabaseManager(dbHost, dbPort, dbName, dbUser, dbPass, dbPoolSize);

        // 2. Circuit Breaker with self-healing callback
        //    When circuit trips OPEN or transitions to HALF_OPEN, it soft-evicts
        //    stale HikariCP connections so the recovery test uses a fresh connection.
        //    This fixes the endurance test issue where stale connections kept the
        //    circuit stuck in OPEN forever.
        CircuitBreaker circuitBreaker = new CircuitBreaker(cbFailureThreshold, cbResetTimeoutMs);
        circuitBreaker.setOnStateChangeCallback(dbManager::softEvictConnections);

        // 3. Stats Aggregator
        StatsAggregator statsAggregator = new StatsAggregator();
        statsAggregator.start(5);

        // 4. Write Buffer (blocking put for zero message loss)
        WriteBuffer writeBuffer = new WriteBuffer(writeBufferCapacity, writeBatchSize);

        // 5. Database Writer (thread pool for batch inserts)
        DatabaseWriter dbWriter = new DatabaseWriter(
                writeBuffer, dbManager, circuitBreaker, statsAggregator,
                writerThreads, flushIntervalMs);
        dbWriter.start();

        // 6. Room Manager
        RoomManager roomManager = new RoomManager();

        // 7. Broadcast WebSocket Server
        BroadcastServer broadcastServer = new BroadcastServer(broadcastWsPort, roomManager);
        broadcastServer.start();
        System.out.println("Broadcast WebSocket server started on port " + broadcastWsPort);

        // 8. Message Consumer Pool
        MessageConsumerPool consumerPool = new MessageConsumerPool(
                roomManager, writeBuffer, statsAggregator, consumerThreads);
        consumerPool.start(rabbitHost, rabbitPort, rabbitUser, rabbitPass, prefetchCount);

        // 9. Metrics API
        MetricsAPI metricsAPI = new MetricsAPI(
                dbManager, statsAggregator, dbWriter, circuitBreaker,
                writeBuffer, consumerPool, roomManager);
        metricsAPI.start(metricsApiPort);

        // ============ SHUTDOWN HOOK ============
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n=== Shutting down Message Processor ===");

            consumerPool.shutdown();

            System.out.println("Waiting for write buffer to drain (depth=" + writeBuffer.size() + ")...");
            long drainStart = System.currentTimeMillis();
            while (!writeBuffer.isEmpty() && System.currentTimeMillis() - drainStart < 120000) {
                try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
                if (writeBuffer.size() % 10000 == 0 || writeBuffer.size() < 1000) {
                    System.out.println("  Buffer draining... remaining: " + writeBuffer.size());
                }
            }
            if (!writeBuffer.isEmpty()) {
                System.err.println("WARNING: " + writeBuffer.size() + " messages still in buffer at shutdown!");
            }

            dbWriter.shutdown();
            metricsAPI.shutdown();
            statsAggregator.shutdown();
            try { broadcastServer.stop(); } catch (Exception ignored) {}
            dbManager.shutdown();

            System.out.println("=== Message Processor shutdown complete ===");
        }));

        System.out.println("\n=== Message Processor ready and listening for messages ===");
        System.out.println("Metrics API: http://localhost:" + metricsApiPort + "/api/all");
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}
