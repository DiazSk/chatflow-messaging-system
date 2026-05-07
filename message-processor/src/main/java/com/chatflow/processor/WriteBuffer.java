package com.chatflow.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write-behind buffer that decouples RabbitMQ consumption from database writes.
 *
 * Design:
 *   - Messages are enqueued by consumer threads
 *   - DatabaseWriter threads drain batches for bulk inserts
 *   - Back-pressure via BLOCKING put: consumer blocks until space is available
 *   - This guarantees zero message loss — consumer naturally throttles to DB write speed
 *   - Buffer capacity should be large enough to absorb bursts
 *     (recommended: >= total messages in a single test run)
 */
public class WriteBuffer {

    private final BlockingQueue<QueueMessage> buffer;
    private final int batchSize;
    private final AtomicLong totalEnqueued = new AtomicLong(0);
    private final AtomicLong totalDropped = new AtomicLong(0);
    private final AtomicLong totalBackPressureWaits = new AtomicLong(0);

    public WriteBuffer(int capacity, int batchSize) {
        this.buffer = new LinkedBlockingQueue<>(capacity);
        this.batchSize = batchSize;
        System.out.println("WriteBuffer initialized: capacity=" + capacity + ", batchSize=" + batchSize);
    }

    /**
     * Enqueue a message for writing. BLOCKS if buffer is full.
     * This creates natural back-pressure: consumer slows to DB write speed.
     * Guarantees zero message loss.
     */
    public boolean put(QueueMessage message) {
        try {
            if (buffer.remainingCapacity() == 0) {
                totalBackPressureWaits.incrementAndGet();
            }
            buffer.put(message);  // Truly blocking — waits forever until space available
            totalEnqueued.incrementAndGet();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            totalDropped.incrementAndGet();
            return false;
        }
    }

    /**
     * Non-blocking offer — used for re-enqueue on retry (DatabaseWriter).
     */
    public boolean offer(QueueMessage message) {
        boolean added = buffer.offer(message);
        if (added) {
            totalEnqueued.incrementAndGet();
        } else {
            totalDropped.incrementAndGet();
        }
        return added;
    }

    /**
     * Drain up to batchSize messages. Non-blocking.
     */
    public List<QueueMessage> drainBatch() {
        List<QueueMessage> batch = new ArrayList<>(batchSize);
        buffer.drainTo(batch, batchSize);
        return batch;
    }

    /**
     * Blocking drain — waits for at least one message, then drains up to batchSize.
     */
    public List<QueueMessage> drainBatchBlocking() throws InterruptedException {
        List<QueueMessage> batch = new ArrayList<>(batchSize);
        QueueMessage first = buffer.take();
        batch.add(first);
        buffer.drainTo(batch, batchSize - 1);
        return batch;
    }

    /**
     * Blocking take — parks the thread at zero CPU until a message arrives.
     * Used by DatabaseWriter as the sole blocking point in the writer loop.
     */
    public QueueMessage take() throws InterruptedException {
        return buffer.take();
    }

    /**
     * Drain up to maxElements additional messages into an existing batch. Non-blocking.
     * Used by adaptive batch sizing to top up a partial drain.
     */
    public int drainTo(Collection<? super QueueMessage> c, int maxElements) {
        return buffer.drainTo(c, maxElements);
    }

    public int size() { return buffer.size(); }
    public boolean isEmpty() { return buffer.isEmpty(); }
    public long getTotalEnqueued() { return totalEnqueued.get(); }
    public long getTotalDropped() { return totalDropped.get(); }
    public long getTotalBackPressureWaits() { return totalBackPressureWaits.get(); }
    public int getBatchSize() { return batchSize; }
}
