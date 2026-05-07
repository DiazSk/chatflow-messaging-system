package com.chatflow.processor;

/**
 * Result of a batch INSERT operation.
 *
 * Separates "attempted" from "inserted" to fix misleading metrics.
 *
 * Problem this solves:
 *   INSERT IGNORE silently skips duplicates. The old code returned only
 *   the count of newly inserted rows. On RabbitMQ redeliveries, most rows
 *   are duplicates, so peakWriteRate showed ~35 msg/s when actual MySQL
 *   throughput was thousands/s. The batches were executing fast — most rows
 *   were just already in the DB.
 *
 * Now we track:
 *   - attempted:    total rows sent to MySQL in the batch
 *   - inserted:     rows actually written (new, non-duplicate)
 *   - duplicates:   rows skipped by INSERT IGNORE (already existed)
 *   - elapsedMs:    wall-clock time for the batch execution
 *
 * StatsAggregator uses "attempted" for write throughput calculations,
 * giving an accurate picture of actual MySQL load.
 */
public class BatchResult {

    private final int attempted;
    private final int inserted;
    private final int duplicates;
    private final long elapsedMs;

    public BatchResult(int attempted, int inserted, long elapsedMs) {
        this.attempted = attempted;
        this.inserted = inserted;
        this.duplicates = attempted - inserted;
        this.elapsedMs = elapsedMs;
    }

    public int getAttempted() { return attempted; }
    public int getInserted() { return inserted; }
    public int getDuplicates() { return duplicates; }
    public long getElapsedMs() { return elapsedMs; }

    @Override
    public String toString() {
        return String.format("BatchResult{attempted=%d, inserted=%d, duplicates=%d, elapsed=%dms}",
                attempted, inserted, duplicates, elapsedMs);
    }
}
