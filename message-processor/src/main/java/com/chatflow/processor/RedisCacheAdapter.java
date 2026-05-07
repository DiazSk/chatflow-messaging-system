package com.chatflow.processor;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis-backed cache adapter.
 *
 * Design principles:
 *   - Fail-open: any Redis error returns null, allowing the in-memory QueryCache
 *     to serve as the fallback. A Redis outage never blocks message processing.
 *   - TTL-only expiry: no explicit prefix invalidation on Redis. Keys expire
 *     naturally (5–30s TTLs), which is sufficient for analytics staleness.
 *     This avoids the O(N) KEYS command and eliminates scan overhead.
 *   - isAvailable() uses a 5-second cooldown between ping checks so a Redis
 *     outage does not spam the connection on every cache miss.
 *   - Pool is small (max 8) because MetricsAPI and consumer threads share it,
 *     but queries are short-lived GET/SETEX operations.
 *
 * Activation: pass -Dredis.host=<ip> when starting the consumer.
 * With Redis disabled (no -Dredis.host), DatabaseManager uses QueryCache only.
 */
public class RedisCacheAdapter {

    private final JedisPool pool;
    private volatile boolean lastAvailable = false;
    private volatile long lastCheckMs = 0;
    private static final long AVAILABILITY_COOLDOWN_MS = 5_000;

    // Metrics
    private final AtomicLong redisHits   = new AtomicLong(0);
    private final AtomicLong redisMisses = new AtomicLong(0);
    private final AtomicLong redisErrors = new AtomicLong(0);

    public RedisCacheAdapter(String host, int port) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(8);
        config.setMaxIdle(4);
        config.setMinIdle(1);
        config.setTestOnBorrow(false);      // skip per-borrow ping for throughput
        config.setTestWhileIdle(true);
        config.setMinEvictableIdleTimeMillis(60_000);
        this.pool = new JedisPool(config, host, port, 2_000);  // 2s connection timeout
        System.out.println("[RedisCacheAdapter] Initialized: " + host + ":" + port);
    }

    /**
     * Get a value from Redis. Returns null on cache miss or any Redis error.
     */
    public String get(String key) {
        try (Jedis jedis = pool.getResource()) {
            String value = jedis.get(key);
            if (value != null) {
                redisHits.incrementAndGet();
            } else {
                redisMisses.incrementAndGet();
            }
            return value;
        } catch (JedisException e) {
            redisErrors.incrementAndGet();
            return null;   // fail-open: caller falls back to in-memory cache
        }
    }

    /**
     * Store a value in Redis with TTL. Silently no-ops on any Redis error.
     */
    public void put(String key, String value, long ttlMs) {
        int ttlSeconds = Math.max(1, (int) (ttlMs / 1000));
        try (Jedis jedis = pool.getResource()) {
            jedis.setex(key, ttlSeconds, value);
        } catch (JedisException e) {
            redisErrors.incrementAndGet();
            // fail-open: in-memory cache will still hold the value
        }
    }

    /**
     * Delete a key from Redis. Used for active cache invalidation on batch commit.
     * Silently no-ops on any Redis error (fail-open: TTL will eventually expire).
     */
    public void delete(String key) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(key);
        } catch (JedisException e) {
            redisErrors.incrementAndGet();
        }
    }

    /**
     * Check whether Redis is reachable, with a 5-second cooldown between pings.
     * This prevents hammering Redis when it is down.
     */
    public boolean isAvailable() {
        long now = System.currentTimeMillis();
        if (now - lastCheckMs < AVAILABILITY_COOLDOWN_MS) {
            return lastAvailable;
        }
        lastCheckMs = now;
        try (Jedis jedis = pool.getResource()) {
            lastAvailable = "PONG".equals(jedis.ping());
        } catch (JedisException e) {
            lastAvailable = false;
        }
        return lastAvailable;
    }

    /**
     * Batch delete keys from Redis in a single network round-trip.
     * Jedis.del(String...) sends one DEL command with all keys.
     */
    public void deleteBatch(java.util.List<String> keys) {
        if (keys == null || keys.isEmpty()) return;
        try (Jedis jedis = pool.getResource()) {
            jedis.del(keys.toArray(new String[0]));
        } catch (JedisException e) {
            redisErrors.incrementAndGet();
            // fail-open: TTL will eventually expire
        }
    }

    // ===================== METRICS =====================

    public long getRedisHits()   { return redisHits.get(); }
    public long getRedisMisses() { return redisMisses.get(); }
    public long getRedisErrors() { return redisErrors.get(); }
    public double getRedisHitRatio() {
        long total = redisHits.get() + redisMisses.get();
        return total > 0 ? (double) redisHits.get() / total : 0.0;
    }

    public void shutdown() {
        if (pool != null && !pool.isClosed()) {
            pool.close();
        }
        System.out.printf("[RedisCacheAdapter] Shut down. Hits: %d, Misses: %d, Errors: %d, HitRatio: %.2f%%%n",
                redisHits.get(), redisMisses.get(), redisErrors.get(), getRedisHitRatio() * 100);
    }
}
