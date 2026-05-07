package com.chatflow.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * REST Metrics API running on the message-processor.
 *
 * Implementation notes:
 *   - All JSON responses built with Gson JsonObject (no more String.format)
 *   - Metrics now show attempted vs inserted vs duplicates separately
 *   - peakWriteRate reflects actual MySQL throughput, not just unique inserts
 *   - Query results from DatabaseManager are proper JsonArray (no more string joining)
 *
 * Endpoints:
 *   GET /api/health                             - System health
 *   GET /api/metrics                            - Comprehensive metrics
 *   GET /api/messages/room?roomId=&start=&end=  - Core Query 1
 *   GET /api/messages/user?userId=&start=&end=  - Core Query 2
 *   GET /api/users/active?start=&end=           - Core Query 3
 *   GET /api/users/rooms?userId=                - Core Query 4
 *   GET /api/analytics/throughput               - Messages per minute
 *   GET /api/analytics/top-users?n=             - Most active users
 *   GET /api/analytics/top-rooms?n=             - Most active rooms
 *   GET /api/analytics/patterns                 - User participation patterns
 *   GET /api/all                                - ALL results in one JSON
 */
public class MetricsAPI {

    private final DatabaseManager dbManager;
    private final StatsAggregator statsAggregator;
    private final DatabaseWriter dbWriter;
    private final CircuitBreaker circuitBreaker;
    private final WriteBuffer writeBuffer;
    private final MessageConsumerPool consumerPool;
    private final RoomManager roomManager;
    private final Gson gson;
    private HttpServer server;

    public MetricsAPI(DatabaseManager dbManager, StatsAggregator statsAggregator,
                      DatabaseWriter dbWriter, CircuitBreaker circuitBreaker,
                      WriteBuffer writeBuffer, MessageConsumerPool consumerPool,
                      RoomManager roomManager) {
        this.dbManager = dbManager;
        this.statsAggregator = statsAggregator;
        this.dbWriter = dbWriter;
        this.circuitBreaker = circuitBreaker;
        this.writeBuffer = writeBuffer;
        this.consumerPool = consumerPool;
        this.roomManager = roomManager;
        this.gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/api/health", this::handleHealth);
        server.createContext("/api/metrics", this::handleMetrics);
        server.createContext("/api/messages/room", this::handleRoomMessages);
        server.createContext("/api/messages/user", this::handleUserMessages);
        server.createContext("/api/users/active", this::handleActiveUsers);
        server.createContext("/api/users/rooms", this::handleUserRooms);
        server.createContext("/api/analytics/throughput", this::handleThroughput);
        server.createContext("/api/analytics/top-users", this::handleTopUsers);
        server.createContext("/api/analytics/top-rooms", this::handleTopRooms);
        server.createContext("/api/analytics/patterns", this::handlePatterns);
        server.createContext("/api/all", this::handleAll);

        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("Metrics API started on port " + port);
    }

    // ===================== HEALTH & METRICS =====================

    private void handleHealth(HttpExchange exchange) throws IOException {
        QueryCache cache = dbManager.getQueryCache();

        JsonObject json = new JsonObject();
        json.addProperty("status", "healthy");
        json.addProperty("service", "ChatFlow Message Processor");
        json.addProperty("circuitBreaker", circuitBreaker.getState().toString());
        json.addProperty("uptime", statsAggregator.getUptimeSeconds());

        JsonObject db = new JsonObject();
        db.addProperty("active", dbManager.getActiveConnections());
        db.addProperty("idle", dbManager.getIdleConnections());
        db.addProperty("total", dbManager.getTotalConnections());
        db.addProperty("waiting", dbManager.getWaitingThreads());
        json.add("dbConnections", db);

        JsonObject readerPool = new JsonObject();
        readerPool.addProperty("active", dbManager.getReaderActiveConnections());
        readerPool.addProperty("idle", dbManager.getReaderIdleConnections());
        readerPool.addProperty("total", dbManager.getReaderTotalConnections());
        readerPool.addProperty("waiting", dbManager.getReaderWaitingThreads());
        json.add("readerPool", readerPool);

        JsonObject cacheObj = new JsonObject();
        cacheObj.addProperty("size", cache.getSize());
        cacheObj.addProperty("hitRatio", cache.getHitRatio());
        json.add("queryCache", cacheObj);

        // Redis cache status (O1/O12 optimization)
        RedisCacheAdapter redis = dbManager.getRedisCacheAdapter();
        if (redis != null) {
            JsonObject redisObj = new JsonObject();
            redisObj.addProperty("enabled", true);
            redisObj.addProperty("available", redis.isAvailable());
            redisObj.addProperty("hits", redis.getRedisHits());
            redisObj.addProperty("misses", redis.getRedisMisses());
            redisObj.addProperty("errors", redis.getRedisErrors());
            redisObj.addProperty("hitRatio", Math.round(redis.getRedisHitRatio() * 10000.0) / 10000.0);
            json.add("redisCache", redisObj);
        } else {
            JsonObject redisObj = new JsonObject();
            redisObj.addProperty("enabled", false);
            json.add("redisCache", redisObj);
        }

        sendResponse(exchange, 200, json.toString());
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        sendResponse(exchange, 200, buildMetricsJson().toString());
    }

    // ===================== CORE QUERIES =====================

    private void handleRoomMessages(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQuery(exchange.getRequestURI());
        try {
            int roomId = Integer.parseInt(params.getOrDefault("roomId", "1"));
            String cursor = params.getOrDefault("cursor", null);
            int limit = Integer.parseInt(params.getOrDefault("limit", "100"));

            long startTime = System.currentTimeMillis();
            JsonArray messages = dbManager.getMessagesForRoom(roomId, cursor, limit);
            long elapsed = System.currentTimeMillis() - startTime;

            // Derive nextCursor from the last row's timestamp for the client to paginate
            String nextCursor = null;
            if (messages.size() == limit) {
                nextCursor = messages.get(messages.size() - 1).getAsJsonObject()
                        .get("timestamp").getAsString();
            }

            JsonObject json = new JsonObject();
            json.addProperty("query", "messagesForRoom");
            json.addProperty("roomId", roomId);
            json.addProperty("limit", limit);
            if (cursor != null) json.addProperty("cursor", cursor);
            if (nextCursor != null) json.addProperty("nextCursor", nextCursor);
            json.addProperty("resultCount", messages.size());
            json.addProperty("queryTimeMs", elapsed);
            json.addProperty("cached", elapsed < 2);
            json.add("messages", messages);

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleUserMessages(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQuery(exchange.getRequestURI());
        try {
            int userId = Integer.parseInt(params.getOrDefault("userId", "1"));
            String cursor = params.getOrDefault("cursor", null);
            int limit = Integer.parseInt(params.getOrDefault("limit", "100"));

            long startTime = System.currentTimeMillis();
            JsonArray messages = dbManager.getUserHistory(userId, cursor, limit);
            long elapsed = System.currentTimeMillis() - startTime;

            String nextCursor = null;
            if (messages.size() == limit) {
                nextCursor = messages.get(messages.size() - 1).getAsJsonObject()
                        .get("timestamp").getAsString();
            }

            JsonObject json = new JsonObject();
            json.addProperty("query", "userHistory");
            json.addProperty("userId", userId);
            json.addProperty("limit", limit);
            if (cursor != null) json.addProperty("cursor", cursor);
            if (nextCursor != null) json.addProperty("nextCursor", nextCursor);
            json.addProperty("resultCount", messages.size());
            json.addProperty("queryTimeMs", elapsed);
            json.addProperty("cached", elapsed < 2);
            json.add("messages", messages);

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleActiveUsers(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQuery(exchange.getRequestURI());
        try {
            String start = params.getOrDefault("start", "2020-01-01 00:00:00");
            String end = params.getOrDefault("end", "2030-12-31 23:59:59");

            long startTime = System.currentTimeMillis();
            int count = dbManager.countActiveUsers(start, end);
            long elapsed = System.currentTimeMillis() - startTime;

            JsonObject json = new JsonObject();
            json.addProperty("query", "activeUsers");
            json.addProperty("startTime", start);
            json.addProperty("endTime", end);
            json.addProperty("activeUserCount", count);
            json.addProperty("queryTimeMs", elapsed);
            json.addProperty("cached", elapsed < 2);

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleUserRooms(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQuery(exchange.getRequestURI());
        try {
            int userId = Integer.parseInt(params.getOrDefault("userId", "1"));

            long startTime = System.currentTimeMillis();
            JsonArray rooms = dbManager.getUserRooms(userId);
            long elapsed = System.currentTimeMillis() - startTime;

            JsonObject json = new JsonObject();
            json.addProperty("query", "userRooms");
            json.addProperty("userId", userId);
            json.addProperty("resultCount", rooms.size());
            json.addProperty("queryTimeMs", elapsed);
            json.addProperty("cached", elapsed < 2);
            json.add("rooms", rooms);

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    // ===================== ANALYTICS =====================

    private void handleThroughput(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQuery(exchange.getRequestURI());
        try {
            int limit = Integer.parseInt(params.getOrDefault("limit", "60"));
            JsonArray stats = dbManager.getMessagesPerMinute(limit);

            JsonObject json = new JsonObject();
            json.addProperty("query", "messagesPerMinute");
            json.addProperty("limit", limit);
            json.add("data", stats);

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleTopUsers(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQuery(exchange.getRequestURI());
        try {
            int n = Integer.parseInt(params.getOrDefault("n", "10"));
            JsonArray users = dbManager.getTopActiveUsers(n);

            JsonObject json = new JsonObject();
            json.addProperty("query", "topActiveUsers");
            json.addProperty("topN", n);
            json.add("users", users);

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handleTopRooms(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQuery(exchange.getRequestURI());
        try {
            int n = Integer.parseInt(params.getOrDefault("n", "10"));
            JsonArray rooms = dbManager.getTopActiveRooms(n);

            JsonObject json = new JsonObject();
            json.addProperty("query", "topActiveRooms");
            json.addProperty("topN", n);
            json.add("rooms", rooms);

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    private void handlePatterns(HttpExchange exchange) throws IOException {
        try {
            JsonArray patterns = dbManager.getUserParticipationPatterns();

            JsonObject json = new JsonObject();
            json.addProperty("query", "userParticipationPatterns");
            json.add("data", patterns);

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    // ===================== ALL-IN-ONE =====================

    private void handleAll(HttpExchange exchange) throws IOException {
        try {
            JsonObject json = new JsonObject();

            // System metrics
            json.add("systemMetrics", buildMetricsJson());

            // Core Query 1 (cursor=null → most recent page)
            long q1Start = System.currentTimeMillis();
            JsonArray roomMsgs = dbManager.getMessagesForRoom(1, null, 100);
            long q1Time = System.currentTimeMillis() - q1Start;
            JsonObject q1 = new JsonObject();
            q1.addProperty("roomId", 1);
            q1.addProperty("count", roomMsgs.size());
            q1.addProperty("queryTimeMs", q1Time);
            json.add("coreQuery1_roomMessages", q1);

            // Core Query 2 (cursor=null → most recent page)
            long q2Start = System.currentTimeMillis();
            JsonArray userMsgs = dbManager.getUserHistory(1, null, 100);
            long q2Time = System.currentTimeMillis() - q2Start;
            JsonObject q2 = new JsonObject();
            q2.addProperty("userId", 1);
            q2.addProperty("count", userMsgs.size());
            q2.addProperty("queryTimeMs", q2Time);
            json.add("coreQuery2_userHistory", q2);

            // Core Query 3
            long q3Start = System.currentTimeMillis();
            int activeUsers = dbManager.countActiveUsers("2020-01-01 00:00:00", "2030-12-31 23:59:59");
            long q3Time = System.currentTimeMillis() - q3Start;
            JsonObject q3 = new JsonObject();
            q3.addProperty("count", activeUsers);
            q3.addProperty("queryTimeMs", q3Time);
            json.add("coreQuery3_activeUsers", q3);

            // Core Query 4
            long q4Start = System.currentTimeMillis();
            JsonArray userRooms = dbManager.getUserRooms(1);
            long q4Time = System.currentTimeMillis() - q4Start;
            JsonObject q4 = new JsonObject();
            q4.addProperty("userId", 1);
            q4.add("rooms", userRooms);
            q4.addProperty("queryTimeMs", q4Time);
            json.add("coreQuery4_userRooms", q4);

            // Analytics
            json.add("analytics_messagesPerMinute", dbManager.getMessagesPerMinute(10));
            json.add("analytics_topUsers", dbManager.getTopActiveUsers(10));
            json.add("analytics_topRooms", dbManager.getTopActiveRooms(10));
            json.add("analytics_participationPatterns", dbManager.getUserParticipationPatterns());

            // Total count
            json.addProperty("totalMessagesInDB", dbManager.getTotalMessageCount());

            sendResponse(exchange, 200, json.toString());
        } catch (Exception e) {
            sendError(exchange, e);
        }
    }

    // ===================== METRICS JSON BUILDER =====================

    /**
     * Build comprehensive metrics JSON using Gson JsonObject.
     * Now includes attempted vs inserted vs duplicates for accurate throughput reporting.
     */
    private JsonObject buildMetricsJson() {
        QueryCache cache = dbManager.getQueryCache();
        JsonObject root = new JsonObject();

        // Throughput — now shows attempted (actual MySQL load) AND inserted (unique new rows)
        JsonObject throughput = new JsonObject();
        throughput.addProperty("currentConsumeRate", round(statsAggregator.getCurrentConsumeRate()));
        throughput.addProperty("currentWriteRate", round(statsAggregator.getCurrentWriteRate()));
        throughput.addProperty("currentInsertRate", round(statsAggregator.getCurrentInsertRate()));
        throughput.addProperty("peakConsumeRate", round(statsAggregator.getPeakConsumeRate()));
        throughput.addProperty("peakWriteRate", round(statsAggregator.getPeakWriteRate()));
        throughput.addProperty("peakInsertRate", round(statsAggregator.getPeakInsertRate()));
        throughput.addProperty("overallThroughput", round(statsAggregator.getOverallThroughput()));
        root.add("throughput", throughput);

        // Totals — attempted vs inserted vs duplicates for clarity
        JsonObject totals = new JsonObject();
        totals.addProperty("messagesConsumed", statsAggregator.getTotalMessagesConsumed());
        totals.addProperty("messagesAttempted", statsAggregator.getTotalMessagesAttempted());
        totals.addProperty("messagesInserted", statsAggregator.getTotalMessagesInserted());
        totals.addProperty("batchesWritten", statsAggregator.getTotalBatchesWritten());
        totals.addProperty("dbInserts", dbManager.getTotalInserted());
        totals.addProperty("dbAttempted", dbManager.getTotalAttempted());
        totals.addProperty("dbFailed", dbManager.getFailedInserts());
        totals.addProperty("duplicatesSkipped", dbManager.getDuplicatesSkipped());
        totals.addProperty("dlqMessages", dbWriter.getTotalDlqMessages());
        root.add("totals", totals);

        // Latency
        JsonObject latency = new JsonObject();
        latency.addProperty("avgBatchWriteMs", round(dbWriter.getAvgWriteLatencyMs()));
        latency.addProperty("maxBatchWriteMs", dbWriter.getMaxWriteLatencyMs());
        latency.addProperty("avgDbBatchMs", round(dbManager.getAvgBatchTimeMs()));
        root.add("latency", latency);

        // Buffer
        JsonObject buffer = new JsonObject();
        buffer.addProperty("currentDepth", dbWriter.getBufferDepth());
        buffer.addProperty("totalEnqueued", writeBuffer.getTotalEnqueued());
        buffer.addProperty("totalDropped", writeBuffer.getTotalDropped());
        root.add("buffer", buffer);

        // Database connections (writer pool)
        JsonObject database = new JsonObject();
        database.addProperty("activeConnections", dbManager.getActiveConnections());
        database.addProperty("idleConnections", dbManager.getIdleConnections());
        database.addProperty("totalConnections", dbManager.getTotalConnections());
        database.addProperty("waitingThreads", dbManager.getWaitingThreads());
        root.add("database", database);

        // Reader pool (isolated pool for MetricsAPI queries)
        JsonObject readerPool = new JsonObject();
        readerPool.addProperty("activeConnections", dbManager.getReaderActiveConnections());
        readerPool.addProperty("idleConnections", dbManager.getReaderIdleConnections());
        readerPool.addProperty("totalConnections", dbManager.getReaderTotalConnections());
        readerPool.addProperty("waitingThreads", dbManager.getReaderWaitingThreads());
        root.add("readerPool", readerPool);

        // Query cache
        JsonObject cacheObj = new JsonObject();
        cacheObj.addProperty("size", cache.getSize());
        cacheObj.addProperty("hits", cache.getHits());
        cacheObj.addProperty("misses", cache.getMisses());
        cacheObj.addProperty("hitRatio", round(cache.getHitRatio()));
        cacheObj.addProperty("evictions", cache.getEvictions());
        root.add("queryCache", cacheObj);

        // Circuit breaker
        JsonObject cb = new JsonObject();
        cb.addProperty("state", circuitBreaker.getState().toString());
        cb.addProperty("failureCount", circuitBreaker.getFailureCount());
        cb.addProperty("totalTrips", circuitBreaker.getTotalTrips());
        root.add("circuitBreaker", cb);

        // Consumer
        JsonObject consumer = new JsonObject();
        consumer.addProperty("messagesConsumed", consumerPool.getMessagesConsumed());
        consumer.addProperty("consumeErrors", consumerPool.getConsumeErrors());
        consumer.addProperty("duplicatesDetected", consumerPool.getDuplicatesDetected());
        root.add("consumer", consumer);

        // Rooms
        JsonObject rooms = new JsonObject();
        rooms.addProperty("activeRooms", roomManager.getActiveRoomCount());
        rooms.addProperty("totalBroadcastConnections", roomManager.getTotalConnections());
        root.add("rooms", rooms);

        root.addProperty("uptime", statsAggregator.getUptimeSeconds());

        // Redis cache metrics (O1/O12 optimization)
        RedisCacheAdapter redis = dbManager.getRedisCacheAdapter();
        JsonObject redisSection = new JsonObject();
        if (redis != null) {
            redisSection.addProperty("enabled", true);
            redisSection.addProperty("available", redis.isAvailable());
            redisSection.addProperty("hits", redis.getRedisHits());
            redisSection.addProperty("misses", redis.getRedisMisses());
            redisSection.addProperty("errors", redis.getRedisErrors());
            redisSection.addProperty("hitRatio", round(redis.getRedisHitRatio()));
        } else {
            redisSection.addProperty("enabled", false);
        }
        root.add("redisCache", redisSection);

        // Summary tables flag
        root.addProperty("summaryTablesEnabled", Boolean.getBoolean("summary.tables"));

        return root;
    }

    /** Round to 1 decimal place for clean JSON output */
    private double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    // ===================== HELPERS =====================

    private Map<String, String> parseQuery(URI uri) {
        Map<String, String> params = new HashMap<>();
        String query = uri.getQuery();
        if (query != null) {
            for (String pair : query.split("&")) {
                String[] kv = pair.split("=", 2);
                if (kv.length == 2) {
                    params.put(kv[0], java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
                }
            }
        }
        return params;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendError(HttpExchange exchange, Exception e) throws IOException {
        JsonObject error = new JsonObject();
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        error.addProperty("error", msg);

        // Classify: 503 for backpressure/circuit-open, 504 for query timeout, 500 for truly unexpected
        int status;
        if (msg.contains("Connection is not available")
                || msg.contains("HikariPool")
                || circuitBreaker.getState() != CircuitBreaker.State.CLOSED) {
            status = 503;  // Service Unavailable — temporary, retry later
            error.addProperty("reason", "backpressure");
            error.addProperty("circuitBreaker", circuitBreaker.getState().toString());
            error.addProperty("bufferDepth", writeBuffer.size());
        } else if (msg.contains("timed out") || msg.contains("Lock wait timeout")) {
            status = 504;  // Gateway Timeout — query took too long
            error.addProperty("reason", "query_timeout");
        } else {
            status = 500;  // Internal Server Error — unexpected
            error.addProperty("reason", "internal_error");
        }

        sendResponse(exchange, status, error.toString());
    }

    public void shutdown() {
        if (server != null) server.stop(0);
        System.out.println("Metrics API stopped.");
    }
}
