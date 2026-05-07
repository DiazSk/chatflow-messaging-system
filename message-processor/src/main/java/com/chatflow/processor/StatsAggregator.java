package com.chatflow.processor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * In-memory statistics aggregator running on its own scheduled thread.
 *
 * Tracks "attempted" (total rows sent to MySQL) separately from
 * "inserted" (new unique rows). peakWriteRate uses the attempted count,
 * giving an accurate picture of actual MySQL throughput. Previously,
 * INSERT IGNORE skipping redelivered duplicates made peakWriteRate show
 * ~35 msg/s when MySQL was actually processing thousands of rows/s.
 *
 * Uses LongAdder for hot counters (better than AtomicLong under high contention).
 */
public class StatsAggregator {

    // Real-time counters (reset every stats window)
    private final LongAdder messagesConsumedWindow = new LongAdder();
    private final LongAdder messagesAttemptedWindow = new LongAdder();  // rows sent to MySQL
    private final LongAdder messagesInsertedWindow = new LongAdder();   // new unique rows

    // Cumulative counters
    private final AtomicLong totalMessagesConsumed = new AtomicLong(0);
    private final AtomicLong totalMessagesAttempted = new AtomicLong(0);
    private final AtomicLong totalMessagesInserted = new AtomicLong(0);
    private final AtomicLong totalBatchesWritten = new AtomicLong(0);
    private final AtomicLong totalWriteTimeMs = new AtomicLong(0);

    // Per-room counters
    private final ConcurrentHashMap<String, LongAdder> messagesPerRoom = new ConcurrentHashMap<>();
    // Per-user counters
    private final ConcurrentHashMap<String, LongAdder> messagesPerUser = new ConcurrentHashMap<>();

    // Throughput snapshots
    private volatile double currentConsumeRate = 0;
    private volatile double currentWriteRate = 0;     // based on ATTEMPTED (actual MySQL load)
    private volatile double currentInsertRate = 0;     // based on INSERTED (new unique rows)
    private volatile double peakConsumeRate = 0;
    private volatile double peakWriteRate = 0;         // peak of attempted rate
    private volatile double peakInsertRate = 0;        // peak of inserted rate

    // Write latency tracking
    private volatile double avgBatchWriteMs = 0;
    private volatile long maxBatchWriteMs = 0;

    private final long startTimeMs;
    private ScheduledExecutorService scheduler;

    public StatsAggregator() {
        this.startTimeMs = System.currentTimeMillis();
    }

    public void start(int intervalSeconds) {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stats-aggregator");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(() -> {
            try { computeRates(intervalSeconds); }
            catch (Exception e) { System.err.println("StatsAggregator error: " + e.getMessage()); }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

        System.out.println("StatsAggregator started: interval=" + intervalSeconds + "s");
    }

    public void recordMessageConsumed(String roomId, String userId) {
        messagesConsumedWindow.increment();
        totalMessagesConsumed.incrementAndGet();
        messagesPerRoom.computeIfAbsent(roomId, k -> new LongAdder()).increment();
        messagesPerUser.computeIfAbsent(userId, k -> new LongAdder()).increment();
    }

    /**
     * Record a batch write to the database.
     * Now tracks attempted (total rows in batch) and inserted (new unique rows) separately.
     */
    public void recordBatchWrite(int attempted, int inserted, long elapsedMs) {
        messagesAttemptedWindow.add(attempted);
        messagesInsertedWindow.add(inserted);
        totalMessagesAttempted.addAndGet(attempted);
        totalMessagesInserted.addAndGet(inserted);
        totalBatchesWritten.incrementAndGet();
        totalWriteTimeMs.addAndGet(elapsedMs);

        if (elapsedMs > maxBatchWriteMs) {
            maxBatchWriteMs = elapsedMs;
        }
    }

    private void computeRates(int intervalSeconds) {
        long consumed = messagesConsumedWindow.sumThenReset();
        long attempted = messagesAttemptedWindow.sumThenReset();
        long inserted = messagesInsertedWindow.sumThenReset();

        currentConsumeRate = (double) consumed / intervalSeconds;
        currentWriteRate = (double) attempted / intervalSeconds;  // actual MySQL throughput
        currentInsertRate = (double) inserted / intervalSeconds;  // unique new rows/s

        if (currentConsumeRate > peakConsumeRate) peakConsumeRate = currentConsumeRate;
        if (currentWriteRate > peakWriteRate) peakWriteRate = currentWriteRate;
        if (currentInsertRate > peakInsertRate) peakInsertRate = currentInsertRate;

        long batches = totalBatchesWritten.get();
        avgBatchWriteMs = batches > 0 ? (double) totalWriteTimeMs.get() / batches : 0;

        if (currentConsumeRate > 0 || currentWriteRate > 0) {
            System.out.printf("[Stats] Consume: %.0f msg/s | Write: %.0f msg/s (%.0f new) | " +
                            "Total consumed: %d | Total attempted: %d | Total inserted: %d | " +
                            "Avg batch: %.1f ms | Users: %d%n",
                    currentConsumeRate, currentWriteRate, currentInsertRate,
                    totalMessagesConsumed.get(), totalMessagesAttempted.get(),
                    totalMessagesInserted.get(), avgBatchWriteMs, messagesPerUser.size());
        }
    }

    // ===================== GETTERS =====================

    public double getCurrentConsumeRate() { return currentConsumeRate; }
    public double getCurrentWriteRate() { return currentWriteRate; }
    public double getCurrentInsertRate() { return currentInsertRate; }
    public double getPeakConsumeRate() { return peakConsumeRate; }
    public double getPeakWriteRate() { return peakWriteRate; }
    public double getPeakInsertRate() { return peakInsertRate; }
    public long getTotalMessagesConsumed() { return totalMessagesConsumed.get(); }
    public long getTotalMessagesAttempted() { return totalMessagesAttempted.get(); }
    public long getTotalMessagesInserted() { return totalMessagesInserted.get(); }
    /** @deprecated Use getTotalMessagesInserted() for unique inserts or getTotalMessagesAttempted() for total */
    public long getTotalMessagesWritten() { return totalMessagesInserted.get(); }
    public long getTotalBatchesWritten() { return totalBatchesWritten.get(); }
    public double getAvgBatchWriteMs() { return avgBatchWriteMs; }
    public long getMaxBatchWriteMs() { return maxBatchWriteMs; }
    public int getUniqueUserCount() { return messagesPerUser.size(); }
    public int getActiveRoomCount() { return messagesPerRoom.size(); }
    public long getUptimeSeconds() { return (System.currentTimeMillis() - startTimeMs) / 1000; }

    public double getOverallThroughput() {
        long seconds = getUptimeSeconds();
        return seconds > 0 ? (double) totalMessagesAttempted.get() / seconds : 0;
    }

    public void shutdown() {
        if (scheduler != null) scheduler.shutdownNow();
        System.out.printf("StatsAggregator shut down. Consumed: %d, Attempted: %d, Inserted: %d, " +
                        "Peak consume: %.0f msg/s, Peak write: %.0f msg/s, Peak insert: %.0f msg/s%n",
                totalMessagesConsumed.get(), totalMessagesAttempted.get(), totalMessagesInserted.get(),
                peakConsumeRate, peakWriteRate, peakInsertRate);
    }
}
