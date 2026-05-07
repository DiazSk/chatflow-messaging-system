package com.chatflow.processor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache stampede (thundering herd) protection using per-key ReentrantLock.
 *
 * Problem: Under 1,000 JMeter users at 70% reads, a cache miss (TTL expiry
 * or active invalidation) causes hundreds of concurrent threads to miss cache
 * simultaneously and ALL fire the same expensive GROUP BY against MySQL on a
 * t3.small. The reader pool saturates instantly, and excess threads queue up
 * on HikariCP's connectionTimeout, causing cascading p99 spikes.
 *
 * Solution: The first thread to miss cache acquires a per-key lock, executes
 * the DB query, and populates the cache. All other threads for the SAME key
 * block on the lock, then re-check cache (which is now warm) and return the
 * cached result without ever touching the database.
 *
 * Memory safety: locks are evicted from the map when no thread holds them,
 * bounded by the number of distinct cache keys (< 50 in our system).
 */
public class StampedeGuard {

    private final ConcurrentHashMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<>();

    /**
     * Acquire a per-key lock. Call this BEFORE the DB query on a cache miss.
     * Returns the lock instance — caller MUST call release() in a finally block.
     */
    public ReentrantLock acquire(String cacheKey) {
        ReentrantLock lock = lockMap.computeIfAbsent(cacheKey, k -> new ReentrantLock());
        try {
            if (!lock.tryLock(3, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new RuntimeException("Timeout acquiring cache lock for key: " + cacheKey);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted acquiring cache lock for key: " + cacheKey, e);
        }
        return lock;
    }

    /**
     * Release the lock. Call in a finally block after the DB query + cache put.
     */
    public void release(String cacheKey, ReentrantLock lock) {
        lock.unlock();
        // Evict lock from map if no one else is waiting (prevents unbounded growth).
        // Safe because computeIfAbsent will recreate it if needed later.
        if (!lock.hasQueuedThreads()) {
            lockMap.remove(cacheKey, lock);
        }
    }

    public int activeLockCount() {
        return lockMap.size();
    }
}