package com.chatflow.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages MySQL connections via HikariCP and provides batch insert + cached query operations.
 *
 * Implementation notes:
 *   - batchInsert() returns BatchResult (attempted vs inserted vs duplicates)
 *     fixing the misleading peakWriteRate metric
 *   - All query methods use Gson JsonArray/JsonObject instead of fragile String.format
 *   - Cache stores proper JSON strings (no <SEP> delimiter)
 *   - HikariCP tuned for connection recovery between endurance rounds:
 *     maxLifetime=180s, keepaliveTime=60s, validationTimeout=5s
 *   - softEvictConnections() exposed for circuit breaker self-healing
 */
public class DatabaseManager {

    // Writer pool: used for batch inserts and DLQ writes.
    // Sized large (DB_POOL_SIZE) for high-throughput batch operations.
    private final HikariDataSource dataSource;

    // Reader pool: used ONLY by MetricsAPI query methods.
    // Completely isolated from writer pool — circuit breaker state and
    // write contention cannot starve MetricsAPI reads.
    // Small (3 connections), fails fast (2s timeout) so API never hangs.
    private final HikariDataSource readerDataSource;

    private final QueryCache queryCache;
    private final RedisCacheAdapter redisCache;   // null when -Dredis.host is absent
    private final Gson gson;
    private final StampedeGuard stampedeGuard = new StampedeGuard();

    // Metrics
    private final AtomicLong totalInserted = new AtomicLong(0);
    private final AtomicLong totalAttempted = new AtomicLong(0);
    private final AtomicLong failedInserts = new AtomicLong(0);
    private final AtomicLong duplicatesSkipped = new AtomicLong(0);
    private final AtomicLong totalBatchTimeMs = new AtomicLong(0);
    private final AtomicLong batchCount = new AtomicLong(0);

    private static final String INSERT_SQL =
            "INSERT IGNORE INTO messages " +
            "(message_id, room_id, user_id, username, message, message_type, timestamp, server_id, client_ip, created_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_DLQ_SQL =
            "INSERT INTO dead_letter_messages (message_id, message_json, error_reason) VALUES (?, ?, ?)";

    // O2: Summary table upserts — maintained per batch insert.
    // Uses writer pool (dataSource), not reader pool — these are writes.
    private static final String UPSERT_USER_SUMMARY =
            "INSERT INTO user_message_summary (user_id, username, total_messages, last_seen) " +
            "VALUES (?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "total_messages = total_messages + VALUES(total_messages), " +
            "last_seen = GREATEST(last_seen, VALUES(last_seen)), " +
            "username = VALUES(username)";

    private static final String UPSERT_ROOM_SUMMARY =
            "INSERT INTO room_message_summary (room_id, total_messages, last_activity) " +
            "VALUES (?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "total_messages = total_messages + VALUES(total_messages), " +
            "last_activity = GREATEST(last_activity, VALUES(last_activity))";

    // O2: flag — enabled by -Dsummary.tables=true
    private final boolean useSummaryTables = Boolean.getBoolean("summary.tables");

    // Cache TTLs
    private static final long ROOM_MESSAGES_CACHE_TTL_MS = 10_000;
    private static final long ACTIVE_USERS_WIDE_CACHE_TTL_MS = 60_000;

    public DatabaseManager(String host, int port, String database, String username, String password,
                           int maxPoolSize) {
        HikariConfig config = new HikariConfig();
        
        String jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + 
                         "?useSSL=false&allowPublicKeyRetrieval=true" +
                         "&rewriteBatchedStatements=true&useServerPrepStmts=false";
        config.setJdbcUrl(jdbcUrl);

        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(maxPoolSize / 2);

        // === CONNECTION RECOVERY FIXES ===
        // Problem: In the endurance test, JDBC connections went stale between rounds
        // because MySQL's wait_timeout (default 600s on our config) expired during idle
        // pauses. HikariCP reused these dead connections, causing batch writes to fail,
        // tripping the circuit breaker to OPEN, and it never recovered.
        //
        // Fix: Aggressive connection lifecycle management:
        config.setConnectionTimeout(10_000);       // 10s — fail fast if pool exhausted
        config.setIdleTimeout(120_000);             // 2min — evict idle connections quickly
        config.setMaxLifetime(180_000);             // 3min — force replacement WELL before MySQL wait_timeout (600s)
        config.setKeepaliveTime(60_000);            // 1min — send SELECT 1 keepalive to prevent MySQL timeout
        config.setValidationTimeout(5_000);         // 5s — fast validation check
        config.setConnectionTestQuery("SELECT 1");  // Validate connections before use
        config.setPoolName("ChatFlow-DB-Pool");

        this.dataSource = new HikariDataSource(config);

        // === READER POOL: scaled for the 70/30 read/write JMeter workload ===
        // Sized for: maxPoolSize=20, connectionTimeout=5s
        //   - 1,000 JMeter users x 70% reads = 700 concurrent read requests
        //   - With stampede protection, most reads are cache hits (sub-1ms)
        //   - Cache misses are serialized per-key, so actual DB concurrency is bounded
        //     by the number of distinct query types (~8-12), not by user count
        //   - 20 connections provides headroom for parallel distinct queries
        //   - MySQL on t3.small (2 vCPUs) can service ~15-20 concurrent queries
        //     before context-switching overhead dominates
        HikariConfig readerConfig = new HikariConfig();
        readerConfig.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database +
                "?useSSL=false&allowPublicKeyRetrieval=true" +
                "&cachePrepStmts=true&prepStmtCacheSize=100" +
                "&prepStmtCacheSqlLimit=2048" +
                "&useServerPrepStmts=true");
        readerConfig.setUsername(username);
        readerConfig.setPassword(password);
        readerConfig.setMaximumPoolSize(20);
        readerConfig.setMinimumIdle(5);                 // warm start
        readerConfig.setConnectionTimeout(5_000);       // queue briefly under burst
        readerConfig.setIdleTimeout(120_000);
        readerConfig.setMaxLifetime(180_000);
        readerConfig.setKeepaliveTime(60_000);
        readerConfig.setValidationTimeout(3_000);
        readerConfig.setConnectionTestQuery("SELECT 1");
        readerConfig.setPoolName("ChatFlow-Reader-Pool");
        readerConfig.setReadOnly(true);                 // MySQL optimization: skip undo-log for reads
        this.readerDataSource = new HikariDataSource(readerConfig);

        this.queryCache = new QueryCache();
        this.gson = new GsonBuilder().serializeNulls().create();

        // O1: Redis read-through cache — enabled only when -Dredis.host is set.
        // Falls back to in-memory QueryCache when absent or unavailable.
        String redisHost = System.getProperty("redis.host", "");
        if (!redisHost.isEmpty()) {
            int redisPort = Integer.parseInt(System.getProperty("redis.port", "6379"));
            this.redisCache = new RedisCacheAdapter(redisHost, redisPort);
            System.out.println("DatabaseManager: Redis cache enabled at " + redisHost + ":" + redisPort);
        } else {
            this.redisCache = null;
            System.out.println("DatabaseManager: Redis cache disabled (set -Dredis.host to enable)");
        }

        System.out.println("DatabaseManager initialized: writerPool=" + maxPoolSize
                + ", readerPool=20, stampedeGuard=enabled"
                + ", host=" + host + ":" + port + "/" + database
                + ", maxLifetime=180s, keepalive=60s, queryCache=enabled");

        // Warm up both pools: force TCP handshake + MySQL thread creation before
        // JMeter ramp-up begins. Without this, the first requests pay ~50ms
        // cold-connection overhead that inflates p99 during the initial seconds.
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");
            System.out.println("[Pool Warm-Up] Writer pool connection verified.");
        } catch (SQLException e) {
            System.err.println("[Pool Warm-Up] Writer pool warm-up failed (non-fatal): " + e.getMessage());
        }
        try (Connection conn = readerDataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");
            System.out.println("[Pool Warm-Up] Reader pool connection verified.");
        } catch (SQLException e) {
            System.err.println("[Pool Warm-Up] Reader pool warm-up failed (non-fatal): " + e.getMessage());
        }
    }

    /**
     * Force soft-eviction of all connections in the pool.
     * Called by CircuitBreaker when transitioning to OPEN or HALF_OPEN.
     * This ensures the next write attempt uses a fresh connection.
     */
    public void softEvictConnections() {
        try {
            dataSource.getHikariPoolMXBean().softEvictConnections();
            System.out.println("[DatabaseManager] Soft-evicted stale connections from pool.");
        } catch (Exception e) {
            System.err.println("[DatabaseManager] Failed to soft-evict: " + e.getMessage());
        }
    }

    // ===================== CACHE HELPERS (O1) =====================
    // getCached: checks Redis first (if enabled), then falls through to in-memory QueryCache.
    // putCached: writes to both Redis and in-memory so the local cache is always warm.
    // invalidateCached: clears in-memory only — Redis entries expire via TTL (no scan needed).

    private String getCached(String key) {
        if (redisCache != null && redisCache.isAvailable()) {
            String v = redisCache.get(key);
            if (v != null) return v;
        }
        return queryCache.get(key);
    }

    private void putCached(String key, String value, long ttlMs) {
        if (redisCache != null && redisCache.isAvailable()) {
            redisCache.put(key, value, ttlMs);
        }
        queryCache.put(key, value, ttlMs);
    }

    /**
     * Active Invalidation: DEL from Redis AND clear in-memory cache.
     *
     * The previous design relied on passive TTL expiry (5-30s staleness).
     * Under the 70/30 JMeter profile, a read arriving 1ms after a batch commit would still
     * see stale analytics for up to 30 seconds. Active DEL ensures the very
     * next read triggers a fresh DB query (protected by StampedeGuard).
     *
     * We delete individual known keys rather than using KEYS/SCAN —
     * O(1) per DEL vs O(N) scan, critical on a shared t3.micro Redis.
     */
    private void invalidateCached(String prefix) {
        // 1. Collect known keys BEFORE clearing in-memory (need the key set)
        java.util.List<String> keysToDelete = queryCache.getKnownKeysWithPrefix(prefix);

        // 2. Clear in-memory (instant, local effect)
        queryCache.invalidatePrefix(prefix);

        // 3. Active Redis invalidation: batch DEL in a single network round-trip
        if (redisCache != null && !keysToDelete.isEmpty()) {
            try {
                redisCache.deleteBatch(keysToDelete);
            } catch (Exception e) {
                // Fail-open: if Redis DEL fails, TTL expiry is the fallback.
                System.err.println("[Cache] Redis batch invalidation failed (non-fatal): " + e.getMessage());
            }
        }
    }

    // ===================== WRITE OPERATIONS =====================

    /**
     * Batch insert messages into MySQL using INSERT IGNORE.
     *
     * Returns BatchResult with separate tracking of attempted vs inserted vs duplicates.
     * This fixes the misleading peakWriteRate metric where INSERT IGNORE skipping
     * duplicates made it look like MySQL was only writing ~35 msg/s when actual
     * batch throughput was thousands/s.
     */
    public BatchResult batchInsert(List<QueueMessage> messages) throws SQLException {
        if (messages == null || messages.isEmpty()) {
            return new BatchResult(0, 0, 0);
        }

        int attempted = messages.size();
        long startTime = System.currentTimeMillis();
        int inserted = 0;
        long elapsed = 0;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

            conn.setAutoCommit(false);

            for (QueueMessage msg : messages) {
                ps.setString(1, msg.getMessageId());
                ps.setInt(2, parseIntSafe(msg.getRoomId(), 0));
                ps.setInt(3, parseIntSafe(msg.getUserId(), 0));
                ps.setString(4, msg.getUsername());
                ps.setString(5, msg.getMessage());
                ps.setString(6, msg.getMessageType());
                ps.setTimestamp(7, parseTimestamp(msg.getTimestamp()));
                ps.setString(8, msg.getServerId());
                ps.setString(9, msg.getClientIp());
                ps.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
                ps.addBatch();
            }

            int[] results = ps.executeBatch();
            conn.commit();

            for (int r : results) {
                // r >= 1: actual row insert confirmed
                // r == Statement.SUCCESS_NO_INFO (-2): insert succeeded but driver
                //   can't report per-row counts (happens with rewriteBatchedStatements=true)
                if (r >= 1 || r == java.sql.Statement.SUCCESS_NO_INFO) inserted++;
            }

            elapsed = System.currentTimeMillis() - startTime;

            // Track metrics
            totalAttempted.addAndGet(attempted);
            totalInserted.addAndGet(inserted);
            duplicatesSkipped.addAndGet(attempted - inserted);
            totalBatchTimeMs.addAndGet(elapsed);
            batchCount.incrementAndGet();

            // Invalidate analytics cache on new data (in-memory only; Redis uses TTL)
            if (inserted > 0) {
                invalidateCached("analytics:");
                invalidateCached("metrics:");
            }

        } catch (SQLException e) {
            failedInserts.addAndGet(attempted);
            throw e;
        }

        // O2: summary upserts need a second connection. Must run only AFTER the insert
        // connection is returned to the pool — otherwise each writer holds two leases at
        // once. With WRITER_THREADS ≈ maximumPoolSize, every thread blocks in
        // getConnection() and Write: 0 msg/s (pool self-deadlock).
        if (useSummaryTables && inserted > 0) {
            updateSummaryTables(messages);
        }

        return new BatchResult(attempted, inserted, elapsed);
    }

    /**
     * O2: Maintain pre-aggregated summary tables after a successful batch insert.
     * Aggregates per-user and per-room counts IN MEMORY from the message list (no DB reads),
     * then batch-upserts to user_message_summary and room_message_summary.
     *
     * Called outside the batch transaction — a failure here is logged and swallowed.
     * The batch insert is already committed; summary tables may lag by at most one batch.
     */
    private void updateSummaryTables(List<QueueMessage> messages) {
        // Aggregate in memory: userId → [username, count, maxTimestamp]
        java.util.Map<Integer, Object[]> userAgg = new java.util.HashMap<>();
        // Aggregate in memory: roomId → [count, maxTimestamp]
        java.util.Map<Integer, Object[]> roomAgg = new java.util.HashMap<>();

        for (QueueMessage msg : messages) {
            int userId = parseIntSafe(msg.getUserId(), 0);
            int roomId = parseIntSafe(msg.getRoomId(), 0);
            Timestamp ts = parseTimestamp(msg.getTimestamp());

            userAgg.merge(userId, new Object[]{msg.getUsername(), 1L, ts}, (existing, incoming) -> {
                existing[1] = (Long) existing[1] + 1;
                Timestamp existTs = (Timestamp) existing[2];
                if (ts.after(existTs)) existing[2] = ts;
                return existing;
            });

            roomAgg.merge(roomId, new Object[]{1L, ts}, (existing, incoming) -> {
                existing[0] = (Long) existing[0] + 1;
                Timestamp existTs = (Timestamp) existing[1];
                if (ts.after(existTs)) existing[1] = ts;
                return existing;
            });
        }

        try (Connection conn = dataSource.getConnection()) {
            // Upsert user summaries
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_USER_SUMMARY)) {
                for (java.util.Map.Entry<Integer, Object[]> e : userAgg.entrySet()) {
                    ps.setInt(1, e.getKey());
                    ps.setString(2, (String) e.getValue()[0]);
                    ps.setLong(3, (Long) e.getValue()[1]);
                    ps.setTimestamp(4, (Timestamp) e.getValue()[2]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // Upsert room summaries
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_ROOM_SUMMARY)) {
                for (java.util.Map.Entry<Integer, Object[]> e : roomAgg.entrySet()) {
                    ps.setInt(1, e.getKey());
                    ps.setLong(2, (Long) e.getValue()[0]);
                    ps.setTimestamp(3, (Timestamp) e.getValue()[1]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        } catch (SQLException e) {
            // Swallow — summary tables are eventually consistent, not transactional
            System.err.println("[DatabaseManager] Summary table update failed (non-fatal): " + e.getMessage());
        }
    }

    public void insertDeadLetter(String messageId, String messageJson, String errorReason) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_DLQ_SQL)) {
            ps.setString(1, messageId);
            ps.setString(2, messageJson);
            ps.setString(3, errorReason);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Failed to insert dead letter: " + e.getMessage());
        }
    }

    // ===================== CACHED QUERY METHODS =====================
    // All methods use Gson JsonObject/JsonArray for type-safe JSON serialization.
    // No more String.format — eliminates broken JSON from special characters.
    // Cache stores json.toString() — no more fragile <SEP> delimiter.

    /**
     * Core Query 1: Get messages for a room using cursor-based (keyset) pagination.
     *
     * Replaces BETWEEN-based range scan with keyset pagination:
     *   - First page (cursor null/empty): most recent messages, ORDER BY timestamp DESC LIMIT ?
     *   - Subsequent pages: WHERE timestamp < cursor ORDER BY timestamp DESC LIMIT ?
     *
     * This eliminates the full-index scan that OFFSET-based or wide BETWEEN queries
     * require for deep pages, keeping query cost constant regardless of page depth.
     *
     * @param roomId           the chat room to query
     * @param cursorTimestamp   ISO-8601 or MySQL timestamp of the last seen row (null for first page)
     * @param limit            max rows to return per page
     */
    public JsonArray getMessagesForRoom(int roomId, String cursorTimestamp, int limit) throws SQLException {
        boolean hasCursor = cursorTimestamp != null && !cursorTimestamp.isEmpty();

        // Paginated (cursor) queries bypass cache entirely.
        // Each cursor value is unique → caching would create unbounded key growth
        // (one key per user per scroll position) with near-zero hit rate.
        if (!hasCursor) {
            String cacheKey = QueryCache.keyRoomMessages(roomId, cursorTimestamp, limit);
            String cached = getCached(cacheKey);
            if (cached != null) {
                return JsonParser.parseString(cached).getAsJsonArray();
            }

            ReentrantLock lock = stampedeGuard.acquire(cacheKey);
            try {
                cached = getCached(cacheKey);
                if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

                JsonArray results = executeRoomMessagesQuery(roomId, null, limit);
                putCached(cacheKey, results.toString(), ROOM_MESSAGES_CACHE_TTL_MS);
                return results;
            } finally {
                stampedeGuard.release(cacheKey, lock);
            }
        }

        // Cursor path: straight to DB, no cache, no lock
        return executeRoomMessagesQuery(roomId, cursorTimestamp, limit);
    }

    /** Shared query logic for getMessagesForRoom — used by both cached and uncached paths. */
    private JsonArray executeRoomMessagesQuery(int roomId, String cursorTimestamp, int limit) throws SQLException {
        boolean hasCursor = cursorTimestamp != null && !cursorTimestamp.isEmpty();
        String sql;

        if (hasCursor) {
            sql = "SELECT message_id, user_id, username, message, message_type, timestamp " +
                  "FROM messages WHERE room_id = ? AND timestamp < ? " +
                  "ORDER BY timestamp DESC LIMIT ?";
        } else {
            sql = "SELECT message_id, user_id, username, message, message_type, timestamp " +
                  "FROM messages WHERE room_id = ? " +
                  "ORDER BY timestamp DESC LIMIT ?";
        }

        JsonArray results = new JsonArray();
        try (Connection conn = readerDataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int paramIdx = 1;
            ps.setInt(paramIdx++, roomId);
            if (hasCursor) {
                ps.setTimestamp(paramIdx++, Timestamp.valueOf(cursorTimestamp));
            }
            ps.setInt(paramIdx, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    JsonObject row = new JsonObject();
                    row.addProperty("messageId", rs.getString("message_id"));
                    row.addProperty("userId", rs.getInt("user_id"));
                    row.addProperty("username", rs.getString("username"));
                    row.addProperty("message", rs.getString("message"));
                    row.addProperty("messageType", rs.getString("message_type"));
                    row.addProperty("timestamp", rs.getTimestamp("timestamp").toString());
                    results.add(row);
                }
            }
        }
        return results;
    }

    /**
     * Core Query 2: Get user's message history using cursor-based (keyset) pagination.
     *
     * @param userId           the user to query
     * @param cursorTimestamp   MySQL timestamp of the last seen row (null for first page)
     * @param limit            max rows to return per page
     */
    public JsonArray getUserHistory(int userId, String cursorTimestamp, int limit) throws SQLException {
        boolean hasCursor = cursorTimestamp != null && !cursorTimestamp.isEmpty();

        // Paginated (cursor) queries bypass cache — same rationale as getMessagesForRoom
        if (!hasCursor) {
            String cacheKey = QueryCache.keyUserHistory(userId, cursorTimestamp, limit);
            String cached = getCached(cacheKey);
            if (cached != null) {
                return JsonParser.parseString(cached).getAsJsonArray();
            }

            ReentrantLock lock = stampedeGuard.acquire(cacheKey);
            try {
                cached = getCached(cacheKey);
                if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

                JsonArray results = executeUserHistoryQuery(userId, null, limit);
                putCached(cacheKey, results.toString(), QueryCache.CORE_QUERY_TTL_MS);
                return results;
            } finally {
                stampedeGuard.release(cacheKey, lock);
            }
        }

        // Cursor path: straight to DB
        return executeUserHistoryQuery(userId, cursorTimestamp, limit);
    }

    /** Shared query logic for getUserHistory — used by both cached and uncached paths. */
    private JsonArray executeUserHistoryQuery(int userId, String cursorTimestamp, int limit) throws SQLException {
        boolean hasCursor = cursorTimestamp != null && !cursorTimestamp.isEmpty();
        String sql;

        if (hasCursor) {
            sql = "SELECT message_id, room_id, message, message_type, timestamp " +
                  "FROM messages WHERE user_id = ? AND timestamp < ? " +
                  "ORDER BY timestamp DESC LIMIT ?";
        } else {
            sql = "SELECT message_id, room_id, message, message_type, timestamp " +
                  "FROM messages WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?";
        }

        JsonArray results = new JsonArray();
        try (Connection conn = readerDataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int paramIdx = 1;
            ps.setInt(paramIdx++, userId);
            if (hasCursor) {
                ps.setTimestamp(paramIdx++, Timestamp.valueOf(cursorTimestamp));
            }
            ps.setInt(paramIdx, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    JsonObject row = new JsonObject();
                    row.addProperty("messageId", rs.getString("message_id"));
                    row.addProperty("roomId", rs.getInt("room_id"));
                    row.addProperty("message", rs.getString("message"));
                    row.addProperty("messageType", rs.getString("message_type"));
                    row.addProperty("timestamp", rs.getTimestamp("timestamp").toString());
                    results.add(row);
                }
            }
        }
        return results;
    }

    /**
     * Core Query 3: Count active users in time window.
     * Uses GROUP BY subquery for loose index scan (30x faster than COUNT(DISTINCT)).
     */
    public int countActiveUsers(String startTime, String endTime) throws SQLException {
        String cacheKey = QueryCache.keyActiveUsers(startTime, endTime);

        String cached = getCached(cacheKey);
        if (cached != null) {
            return Integer.parseInt(cached);
        }

        ReentrantLock lock = stampedeGuard.acquire(cacheKey);
        try {
            cached = getCached(cacheKey);
            if (cached != null) return Integer.parseInt(cached);

            boolean isWideRange = isAllTimeRange(startTime, endTime);
            String sql;

            if (isWideRange) {
                sql = "SELECT COUNT(*) AS cnt FROM (SELECT user_id FROM messages GROUP BY user_id) AS sub";
            } else {
                sql = "SELECT COUNT(*) AS cnt FROM (" +
                      "SELECT user_id FROM messages WHERE timestamp BETWEEN ? AND ? GROUP BY user_id" +
                      ") AS sub";
            }

            try (Connection conn = readerDataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                if (!isWideRange) {
                    ps.setTimestamp(1, Timestamp.valueOf(startTime));
                    ps.setTimestamp(2, Timestamp.valueOf(endTime));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    int count = rs.next() ? rs.getInt("cnt") : 0;
                    long ttl = isWideRange ? ACTIVE_USERS_WIDE_CACHE_TTL_MS : QueryCache.CORE_QUERY_TTL_MS;
                    putCached(cacheKey, String.valueOf(count), ttl);
                    return count;
                }
            }
        } finally {
            stampedeGuard.release(cacheKey, lock);
        }
    }

    private boolean isAllTimeRange(String startTime, String endTime) {
        try {
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime start = LocalDateTime.parse(startTime, fmt);
            LocalDateTime end = LocalDateTime.parse(endTime, fmt);
            return ChronoUnit.DAYS.between(start, end) > 365;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Core Query 4: Get rooms user has participated in.
     */
    public JsonArray getUserRooms(int userId) throws SQLException {
        String cacheKey = QueryCache.keyUserRooms(userId);

        String cached = getCached(cacheKey);
        if (cached != null) {
            return JsonParser.parseString(cached).getAsJsonArray();
        }

        ReentrantLock lock = stampedeGuard.acquire(cacheKey);
        try {
            cached = getCached(cacheKey);
            if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

            String sql = "SELECT room_id, MAX(timestamp) AS last_activity, COUNT(*) AS msg_count " +
                         "FROM messages WHERE user_id = ? GROUP BY room_id ORDER BY last_activity DESC";

            JsonArray results = new JsonArray();
            try (Connection conn = readerDataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, userId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        JsonObject row = new JsonObject();
                        row.addProperty("roomId", rs.getInt("room_id"));
                        row.addProperty("lastActivity", rs.getTimestamp("last_activity").toString());
                        row.addProperty("messageCount", rs.getInt("msg_count"));
                        results.add(row);
                    }
                }
            }

            putCached(cacheKey, results.toString(), QueryCache.CORE_QUERY_TTL_MS);
            return results;
        } finally {
            stampedeGuard.release(cacheKey, lock);
        }
    }

    // ===================== ANALYTICS QUERIES =====================

    public JsonArray getTopActiveUsers(int topN) throws SQLException {
        String cacheKey = QueryCache.keyTopUsers(topN);

        String cached = getCached(cacheKey);
        if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

        ReentrantLock lock = stampedeGuard.acquire(cacheKey);
        try {
            cached = getCached(cacheKey);
            if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

            // O2 fast path: summary table read is O(num_users), not O(all_messages)
            if (useSummaryTables) {
                String sql = "SELECT user_id, username, total_messages AS msg_count " +
                             "FROM user_message_summary ORDER BY total_messages DESC LIMIT ?";
                JsonArray results = new JsonArray();
                try (Connection conn = readerDataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setInt(1, topN);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            JsonObject row = new JsonObject();
                            row.addProperty("userId", rs.getInt("user_id"));
                            row.addProperty("username", rs.getString("username"));
                            row.addProperty("messageCount", rs.getLong("msg_count"));
                            results.add(row);
                        }
                    }
                }
                putCached(cacheKey, results.toString(), QueryCache.ANALYTICS_TTL_MS);
                return results;
            }

            // Fallback: full-table GROUP BY (baseline path)
            String sql = "SELECT user_id, username, COUNT(*) AS msg_count " +
                         "FROM messages GROUP BY user_id, username ORDER BY msg_count DESC LIMIT ?";

            JsonArray results = new JsonArray();
            try (Connection conn = readerDataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, topN);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        JsonObject row = new JsonObject();
                        row.addProperty("userId", rs.getInt("user_id"));
                        row.addProperty("username", rs.getString("username"));
                        row.addProperty("messageCount", rs.getInt("msg_count"));
                        results.add(row);
                    }
                }
            }
            putCached(cacheKey, results.toString(), QueryCache.ANALYTICS_TTL_MS);
            return results;
        } finally {
            stampedeGuard.release(cacheKey, lock);
        }
    }

    public JsonArray getTopActiveRooms(int topN) throws SQLException {
        String cacheKey = QueryCache.keyTopRooms(topN);

        String cached = getCached(cacheKey);
        if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

        ReentrantLock lock = stampedeGuard.acquire(cacheKey);
        try {
            cached = getCached(cacheKey);
            if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

            // O2 fast path: summary table read is O(num_rooms), not O(all_messages)
            if (useSummaryTables) {
                String sql = "SELECT room_id, total_messages AS msg_count " +
                             "FROM room_message_summary ORDER BY total_messages DESC LIMIT ?";
                JsonArray results = new JsonArray();
                try (Connection conn = readerDataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setInt(1, topN);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            JsonObject row = new JsonObject();
                            row.addProperty("roomId", rs.getInt("room_id"));
                            row.addProperty("messageCount", rs.getLong("msg_count"));
                            results.add(row);
                        }
                    }
                }
                putCached(cacheKey, results.toString(), QueryCache.ANALYTICS_TTL_MS);
                return results;
            }

            // Fallback: full-table GROUP BY (baseline path)
            String sql = "SELECT room_id, COUNT(*) AS msg_count, COUNT(DISTINCT user_id) AS unique_users " +
                         "FROM messages GROUP BY room_id ORDER BY msg_count DESC LIMIT ?";

            JsonArray results = new JsonArray();
            try (Connection conn = readerDataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, topN);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        JsonObject row = new JsonObject();
                        row.addProperty("roomId", rs.getInt("room_id"));
                        row.addProperty("messageCount", rs.getInt("msg_count"));
                        row.addProperty("uniqueUsers", rs.getInt("unique_users"));
                        results.add(row);
                    }
                }
            }
            putCached(cacheKey, results.toString(), QueryCache.ANALYTICS_TTL_MS);
            return results;
        } finally {
            stampedeGuard.release(cacheKey, lock);
        }
    }

    public JsonArray getMessagesPerMinute(int limitMinutes) throws SQLException {
        String cacheKey = QueryCache.keyMessagesPerMinute(limitMinutes);

        String cached = getCached(cacheKey);
        if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

        ReentrantLock lock = stampedeGuard.acquire(cacheKey);
        try {
            cached = getCached(cacheKey);
            if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

            String sql = "SELECT DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') AS minute_bucket, " +
                         "COUNT(*) AS msg_count " +
                         "FROM messages GROUP BY minute_bucket ORDER BY minute_bucket DESC LIMIT ?";

            JsonArray results = new JsonArray();
            try (Connection conn = readerDataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, limitMinutes);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        JsonObject row = new JsonObject();
                        row.addProperty("minute", rs.getString("minute_bucket"));
                        row.addProperty("messageCount", rs.getInt("msg_count"));
                        results.add(row);
                    }
                }
            }
            putCached(cacheKey, results.toString(), QueryCache.ANALYTICS_TTL_MS);
            return results;
        } finally {
            stampedeGuard.release(cacheKey, lock);
        }
    }

    public JsonArray getUserParticipationPatterns() throws SQLException {
        String cacheKey = QueryCache.keyParticipationPatterns();

        String cached = getCached(cacheKey);
        if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

        ReentrantLock lock = stampedeGuard.acquire(cacheKey);
        try {
            cached = getCached(cacheKey);
            if (cached != null) return JsonParser.parseString(cached).getAsJsonArray();

            String sql = "SELECT HOUR(timestamp) AS hour_of_day, COUNT(*) AS msg_count, " +
                         "COUNT(DISTINCT user_id) AS unique_users " +
                         "FROM messages GROUP BY hour_of_day ORDER BY hour_of_day";

            JsonArray results = new JsonArray();
            try (Connection conn = readerDataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        JsonObject row = new JsonObject();
                        row.addProperty("hourOfDay", rs.getInt("hour_of_day"));
                        row.addProperty("messageCount", rs.getInt("msg_count"));
                        row.addProperty("uniqueUsers", rs.getInt("unique_users"));
                        results.add(row);
                    }
                }
            }
            putCached(cacheKey, results.toString(), QueryCache.ANALYTICS_TTL_MS);
            return results;
        } finally {
            stampedeGuard.release(cacheKey, lock);
        }
    }

    public long getTotalMessageCount() throws SQLException {
        String cacheKey = QueryCache.keyTotalCount();

        String cached = getCached(cacheKey);
        if (cached != null) return Long.parseLong(cached);

        ReentrantLock lock = stampedeGuard.acquire(cacheKey);
        try {
            cached = getCached(cacheKey);
            if (cached != null) return Long.parseLong(cached);

            try (Connection conn = readerDataSource.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM messages")) {
                long count = rs.next() ? rs.getLong(1) : 0;
                putCached(cacheKey, String.valueOf(count), QueryCache.METRICS_TTL_MS);
                return count;
            }
        } finally {
            stampedeGuard.release(cacheKey, lock);
        }
    }

    // ===================== METRICS =====================

    public long getTotalInserted() { return totalInserted.get(); }
    public long getTotalAttempted() { return totalAttempted.get(); }
    public long getFailedInserts() { return failedInserts.get(); }
    public long getDuplicatesSkipped() { return duplicatesSkipped.get(); }
    public double getAvgBatchTimeMs() {
        long count = batchCount.get();
        return count > 0 ? (double) totalBatchTimeMs.get() / count : 0;
    }
    // Writer pool metrics
    public int getActiveConnections() { return dataSource.getHikariPoolMXBean().getActiveConnections(); }
    public int getIdleConnections() { return dataSource.getHikariPoolMXBean().getIdleConnections(); }
    public int getTotalConnections() { return dataSource.getHikariPoolMXBean().getTotalConnections(); }
    public int getWaitingThreads() { return dataSource.getHikariPoolMXBean().getThreadsAwaitingConnection(); }

    // Reader pool metrics (isolated pool used by MetricsAPI queries)
    public int getReaderActiveConnections() { return readerDataSource.getHikariPoolMXBean().getActiveConnections(); }
    public int getReaderIdleConnections() { return readerDataSource.getHikariPoolMXBean().getIdleConnections(); }
    public int getReaderTotalConnections() { return readerDataSource.getHikariPoolMXBean().getTotalConnections(); }
    public int getReaderWaitingThreads() { return readerDataSource.getHikariPoolMXBean().getThreadsAwaitingConnection(); }

    public QueryCache getQueryCache() { return queryCache; }
    public RedisCacheAdapter getRedisCacheAdapter() { return redisCache; }

    // ===================== HELPERS =====================

    private int parseIntSafe(String value, int defaultVal) {
        try { return Integer.parseInt(value); }
        catch (Exception e) { return defaultVal; }
    }

    private Timestamp parseTimestamp(String isoTimestamp) {
        try {
            Instant instant = Instant.parse(isoTimestamp);
            return Timestamp.from(instant);
        } catch (Exception e) {
            return new Timestamp(System.currentTimeMillis());
        }
    }

    public void shutdown() {
        if (redisCache != null) redisCache.shutdown();
        if (queryCache != null) queryCache.shutdown();
        if (readerDataSource != null && !readerDataSource.isClosed()) readerDataSource.close();
        if (dataSource != null && !dataSource.isClosed()) dataSource.close();
        System.out.println("DatabaseManager shut down. Attempted: " + totalAttempted.get()
                + ", Inserted: " + totalInserted.get()
                + ", Duplicates: " + duplicatesSkipped.get()
                + ", Failed: " + failedInserts.get());
    }
}