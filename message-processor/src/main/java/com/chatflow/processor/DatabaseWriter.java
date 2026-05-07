package com.chatflow.processor;

import com.google.gson.Gson;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Separate thread pool for database write operations.
 *
 * Architecture (write-behind pattern):
 *   Consumer threads → WriteBuffer → DatabaseWriter threads → MySQL
 *                                                          ↘ DeadLetterQueue (on failure)
 *
 * Implementation notes:
 *   - Uses BatchResult to track attempted vs inserted vs duplicates separately
 *   - peakWriteRate now reflects actual MySQL throughput (attempted rows/s),
 *     not just unique new inserts which was misleadingly low due to INSERT IGNORE
 *     skipping redelivered duplicates
 *   - Exponential backoff with proper cap
 *   - Circuit breaker integration with self-healing (pool refresh on OPEN)
 */
public class DatabaseWriter {

    private static final String DLQ_FALLBACK_FILE = "dlq-fallback.log";

    private final WriteBuffer writeBuffer;
    private final DatabaseManager dbManager;
    private final CircuitBreaker circuitBreaker;
    private final StatsAggregator statsAggregator;
    private final Gson gson = new Gson();

    private final int writerThreads;
    private final long flushIntervalMs;
    private ExecutorService writerPool;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Metrics — now tracks attempted AND inserted separately
    private final AtomicLong totalBatchesWritten = new AtomicLong(0);
    private final AtomicLong totalBatchesFailed = new AtomicLong(0);
    private final AtomicLong totalMessagesAttempted = new AtomicLong(0);
    private final AtomicLong totalMessagesInserted = new AtomicLong(0);
    private final AtomicLong totalDlqMessages = new AtomicLong(0);
    private final AtomicLong consecutiveFailures = new AtomicLong(0);

    // Write latency tracking
    private final AtomicLong totalWriteLatencyMs = new AtomicLong(0);
    private final AtomicLong writeLatencyCount = new AtomicLong(0);
    private volatile long maxWriteLatencyMs = 0;

    public DatabaseWriter(WriteBuffer writeBuffer, DatabaseManager dbManager,
                          CircuitBreaker circuitBreaker, StatsAggregator statsAggregator,
                          int writerThreads, long flushIntervalMs) {
        this.writeBuffer = writeBuffer;
        this.dbManager = dbManager;
        this.circuitBreaker = circuitBreaker;
        this.statsAggregator = statsAggregator;
        this.writerThreads = writerThreads;
        this.flushIntervalMs = flushIntervalMs;
    }

    public void start() {
        running.set(true);
        writerPool = Executors.newFixedThreadPool(writerThreads);

        for (int i = 0; i < writerThreads; i++) {
            final int threadId = i;
            writerPool.submit(() -> writerLoop(threadId));
        }

        System.out.println("DatabaseWriter started: threads=" + writerThreads
                + ", flushInterval=" + flushIntervalMs + "ms"
                + ", batchSize=" + writeBuffer.getBatchSize());
    }

    // Adaptive batch sizing is now inline in writerLoop (STEP 3)

    private void writerLoop(int threadId) {
        while (running.get()) {
            try {
                // STEP 1: Block until at least one message arrives.
                // This is the ONLY blocking point — thread sleeps here at zero CPU
                // when the buffer is empty, fixing the busy-wait spin.
                QueueMessage firstMsg = writeBuffer.take();

                // STEP 2: Build the batch starting with the message we just got
                java.util.ArrayList<QueueMessage> batch = new java.util.ArrayList<>();
                batch.add(firstMsg);

                // STEP 3: Adaptive sizing based on current queue depth
                int currentDepth = writeBuffer.size();
                int limit = (currentDepth > 10000) ? 5000
                          : (currentDepth > 2000)  ? 2000
                          :                          500;

                // STEP 4: Non-blocking drain of up to (limit - 1) more messages
                writeBuffer.drainTo(batch, limit - 1);

                writeBatch(batch, threadId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[Writer-" + threadId + "] Unexpected error: " + e.getMessage());
                sleepSafe(1000);
            }
        }

        // Final flush on shutdown
        List<QueueMessage> remaining = writeBuffer.drainBatch();
        if (!remaining.isEmpty()) {
            try {
                writeBatch(remaining, threadId);
            } catch (Exception e) {
                System.err.println("[Writer-" + threadId + "] Final flush failed: " + e.getMessage());
                sendToDlq(remaining, "Shutdown flush failed: " + e.getMessage());
            }
        }

        System.out.println("[Writer-" + threadId + "] Stopped.");
    }

    private void writeBatch(List<QueueMessage> batch, int threadId) {
        // Check circuit breaker
        if (!circuitBreaker.allowRequest()) {
            sendToDlq(batch, "Circuit breaker OPEN");
            totalBatchesFailed.incrementAndGet();
            return;
        }

        try {
            // batchInsert now returns BatchResult with attempted/inserted/duplicates
            BatchResult result = dbManager.batchInsert(batch);

            // Track write latency
            totalWriteLatencyMs.addAndGet(result.getElapsedMs());
            writeLatencyCount.incrementAndGet();
            if (result.getElapsedMs() > maxWriteLatencyMs) {
                maxWriteLatencyMs = result.getElapsedMs();
            }

            // Success — update all counters
            circuitBreaker.recordSuccess();
            consecutiveFailures.set(0);
            totalBatchesWritten.incrementAndGet();
            totalMessagesAttempted.addAndGet(result.getAttempted());
            totalMessagesInserted.addAndGet(result.getInserted());

            // StatsAggregator now gets BOTH attempted and inserted
            // so peakWriteRate reflects actual MySQL throughput
            statsAggregator.recordBatchWrite(result.getAttempted(), result.getInserted(), result.getElapsedMs());

        } catch (SQLException e) {
            circuitBreaker.recordFailure();
            long failures = consecutiveFailures.incrementAndGet();
            totalBatchesFailed.incrementAndGet();

            System.err.println("[Writer-" + threadId + "] Batch write failed (attempt " + failures
                    + "): " + e.getMessage());

            // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s (capped)
            long backoffMs = Math.min(1000 * (1L << Math.min(failures - 1, 5)), 30000);
            sleepSafe(backoffMs);

            // After 3 consecutive failures, send to DLQ instead of infinite retry
            if (failures >= 3) {
                sendToDlq(batch, "Consecutive failures: " + failures + " | " + e.getMessage());
            } else {
                // Re-enqueue for retry
                for (QueueMessage msg : batch) {
                    writeBuffer.offer(msg);
                }
            }
        }
    }

    private void sendToDlq(List<QueueMessage> messages, String reason) {
        for (QueueMessage msg : messages) {
            try {
                String json = gson.toJson(msg);
                dbManager.insertDeadLetter(
                        msg.getMessageId() != null ? msg.getMessageId() : "unknown",
                        json, reason);
                totalDlqMessages.incrementAndGet();
            } catch (Exception e) {
                System.err.println("DLQ insert failed for message " + msg.getMessageId()
                        + ": " + e.getMessage());
                appendToLocalDlqFile(msg, reason, e.getMessage());
            }
        }
    }

    /**
     * Last-resort fallback: when the DB is completely unreachable and even the
     * dead_letter_messages INSERT fails, write the message JSON to a local file
     * so it can be replayed later. Without this, messages are silently lost.
     */
    private void appendToLocalDlqFile(QueueMessage msg, String reason, String error) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DLQ_FALLBACK_FILE, true))) {
            String json = gson.toJson(msg);
            writer.write(Instant.now().toString()
                    + " | msgId=" + (msg.getMessageId() != null ? msg.getMessageId() : "unknown")
                    + " | reason=" + reason
                    + " | dbError=" + error
                    + " | payload=" + json);
            writer.newLine();
        } catch (IOException ioe) {
            System.err.println("CRITICAL: Cannot write to local DLQ fallback file: " + ioe.getMessage());
        }
    }

    private void sleepSafe(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public void shutdown() {
        running.set(false);
        if (writerPool != null) {
            writerPool.shutdown();
            try {
                if (!writerPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    writerPool.shutdownNow();
                    writerPool.awaitTermination(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                writerPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("DatabaseWriter shut down. Batches: " + totalBatchesWritten.get()
                + ", Attempted: " + totalMessagesAttempted.get()
                + ", Inserted: " + totalMessagesInserted.get()
                + ", Failed batches: " + totalBatchesFailed.get()
                + ", DLQ: " + totalDlqMessages.get());
    }

    // Metrics
    public long getTotalBatchesWritten() { return totalBatchesWritten.get(); }
    public long getTotalBatchesFailed() { return totalBatchesFailed.get(); }
    public long getTotalMessagesAttempted() { return totalMessagesAttempted.get(); }
    public long getTotalMessagesInserted() { return totalMessagesInserted.get(); }
    public long getTotalDlqMessages() { return totalDlqMessages.get(); }
    public int getBufferDepth() { return writeBuffer.size(); }
    public double getAvgWriteLatencyMs() {
        long count = writeLatencyCount.get();
        return count > 0 ? (double) totalWriteLatencyMs.get() / count : 0;
    }
    public long getMaxWriteLatencyMs() { return maxWriteLatencyMs; }
}