package com.chatflow.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory query result cache with TTL-based expiration.
 *
 * Caches results of expensive database queries (analytics, aggregations)
 * to reduce MySQL load during concurrent API calls.
 *
 * Design decisions:
 *   - ConcurrentHashMap for thread-safe access from API handler threads
 *   - Per-entry TTL: short for core queries (5s), longer for analytics (30s)
 *   - Periodic eviction thread to prevent memory leaks
 *   - Cache key = query identifier + parameters
 *   - Tracks hit/miss ratio for performance monitoring
 *
 * As recommended by the spec: "Consider query result caching"
 */
public class QueryCache {

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService evictor;

    // Metrics
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);

    // Default TTLs
    public static final long CORE_QUERY_TTL_MS = 5_000;       // 5 seconds for core queries
    public static final long ANALYTICS_TTL_MS = 30_000;        // 30 seconds for analytics
    public static final long METRICS_TTL_MS = 3_000;           // 3 seconds for system metrics

    private static class CacheEntry {
        final String value;
        final long expiresAt;

        CacheEntry(String value, long ttlMs) {
            this.value = value;
            this.expiresAt = System.currentTimeMillis() + ttlMs;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expiresAt;
        }
    }

    public QueryCache() {
        // Evict expired entries every 10 seconds
        this.evictor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-evictor");
            t.setDaemon(true);
            return t;
        });
        this.evictor.scheduleAtFixedRate(this::evictExpired, 10, 10, TimeUnit.SECONDS);
        System.out.println("QueryCache initialized: coreTTL=" + CORE_QUERY_TTL_MS + "ms"
                + ", analyticsTTL=" + ANALYTICS_TTL_MS + "ms");
    }

    /**
     * Get a cached value by key. Returns null if not found or expired.
     */
    public String get(String key) {
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            misses.incrementAndGet();
            return null;
        }
        if (entry.isExpired()) {
            cache.remove(key);
            misses.incrementAndGet();
            evictions.incrementAndGet();
            return null;
        }
        hits.incrementAndGet();
        return entry.value;
    }

    /**
     * Put a value into the cache with a specific TTL.
     */
    public void put(String key, String value, long ttlMs) {
        cache.put(key, new CacheEntry(value, ttlMs));
    }

    /**
     * Invalidate all entries (useful after bulk writes).
     */
    public void invalidateAll() {
        cache.clear();
    }

    /**
     * Invalidate entries matching a prefix (e.g., "analytics:" to clear all analytics cache).
     */
    public void invalidatePrefix(String prefix) {
        cache.keySet().removeIf(key -> key.startsWith(prefix));
    }

    /**
     * Return all cache keys matching a prefix. Used to drive targeted Redis DEL
     * commands during active invalidation, avoiding O(N) KEYS/SCAN on Redis.
     */
    public List<String> getKnownKeysWithPrefix(String prefix) {
        List<String> matching = new ArrayList<>();
        for (String key : cache.keySet()) {
            if (key.startsWith(prefix)) {
                matching.add(key);
            }
        }
        return matching;
    }

    /**
     * Remove expired entries from the cache.
     */
    private void evictExpired() {
        int before = cache.size();
        cache.entrySet().removeIf(entry -> entry.getValue().isExpired());
        int removed = before - cache.size();
        if (removed > 0) {
            evictions.addAndGet(removed);
        }
    }

    // ===================== CACHE KEY BUILDERS =====================

    public static String keyRoomMessages(int roomId, String cursor, int limit) {
        return "core:room:" + roomId + ":" + (cursor != null ? cursor : "latest") + ":" + limit;
    }

    public static String keyUserHistory(int userId, String cursor, int limit) {
        return "core:user:" + userId + ":" + (cursor != null ? cursor : "latest") + ":" + limit;
    }

    public static String keyActiveUsers(String start, String end) {
        return "core:activeUsers:" + start + ":" + end;
    }

    public static String keyUserRooms(int userId) {
        return "core:userRooms:" + userId;
    }

    public static String keyTopUsers(int n) {
        return "analytics:topUsers:" + n;
    }

    public static String keyTopRooms(int n) {
        return "analytics:topRooms:" + n;
    }

    public static String keyMessagesPerMinute(int limit) {
        return "analytics:msgPerMin:" + limit;
    }

    public static String keyParticipationPatterns() {
        return "analytics:patterns";
    }

    public static String keyTotalCount() {
        return "metrics:totalCount";
    }

    // ===================== METRICS =====================

    public long getHits() { return hits.get(); }
    public long getMisses() { return misses.get(); }
    public long getEvictions() { return evictions.get(); }
    public int getSize() { return cache.size(); }
    public double getHitRatio() {
        long total = hits.get() + misses.get();
        return total > 0 ? (double) hits.get() / total : 0;
    }

    public void shutdown() {
        if (evictor != null) {
            evictor.shutdownNow();
        }
        System.out.printf("QueryCache shut down. Hits: %d, Misses: %d, Hit ratio: %.2f%%, Evictions: %d%n",
                hits.get(), misses.get(), getHitRatio() * 100, evictions.get());
    }
}
