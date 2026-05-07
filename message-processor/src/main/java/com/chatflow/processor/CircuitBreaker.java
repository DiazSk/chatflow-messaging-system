package com.chatflow.processor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Circuit breaker for database write operations.
 * Prevents cascading failures when MySQL is unavailable or overloaded.
 *
 * States:
 *   CLOSED    - Normal operation, writes pass through
 *   OPEN      - Too many failures, writes rejected immediately (sent to DLQ)
 *   HALF_OPEN - After timeout, allow one test write to check recovery
 *
 *  Self-healing behavior:
 *   - When the circuit trips OPEN, it invokes an onOpenCallback which
 *     soft-evicts stale connections from HikariCP. This ensures the
 *     HALF_OPEN test uses a fresh connection instead of a stale one
 *     that would fail again, trapping the circuit in OPEN forever.
 *   - Root cause this fixes: In the endurance test, JDBC connections
 *     went stale between rounds (MySQL wait_timeout expired). The old
 *     circuit breaker tripped OPEN and never recovered because the
 *     HALF_OPEN test reused the same dead connections from the pool.
 */
public class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private final AtomicLong totalTrips = new AtomicLong(0);

    private final int failureThreshold;
    private final long resetTimeoutMs;

    /**
     * Callback invoked when circuit transitions to OPEN or HALF_OPEN.
     * Used to soft-evict stale connections from HikariCP so that the
     * HALF_OPEN test gets a fresh connection.
     */
    private volatile Runnable onStateChangeCallback;

    /**
     * @param failureThreshold consecutive failures before opening
     * @param resetTimeoutMs   time before transitioning to HALF_OPEN
     */
    public CircuitBreaker(int failureThreshold, long resetTimeoutMs) {
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
    }

    /**
     * Register a callback for state changes (typically: soft-evict HikariCP connections).
     * Called when circuit opens AND when transitioning to HALF_OPEN.
     */
    public void setOnStateChangeCallback(Runnable callback) {
        this.onStateChangeCallback = callback;
    }

    public boolean allowRequest() {
        State current = state.get();

        if (current == State.CLOSED) {
            return true;
        }

        if (current == State.OPEN) {
            if (System.currentTimeMillis() - lastFailureTime.get() > resetTimeoutMs) {
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    System.out.println("[CircuitBreaker] OPEN → HALF_OPEN. Testing with fresh connection...");
                    invokeCallback();
                    // Give HikariCP time to create a fresh connection after eviction.
                    // Without this delay the test write grabs the same degraded connection
                    // that was just soft-evicted, fails immediately, and traps the CB in OPEN.
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
                return true;
            }
            return false;
        }

        // HALF_OPEN: allow one test request
        return true;
    }

    public void recordSuccess() {
        State prev = state.getAndSet(State.CLOSED);
        failureCount.set(0);
        if (prev != State.CLOSED) {
            System.out.println("[CircuitBreaker] " + prev + " → CLOSED. Recovery confirmed.");
        }
    }

    public void recordFailure() {
        int failures = failureCount.incrementAndGet();
        lastFailureTime.set(System.currentTimeMillis());

        if (failures >= failureThreshold) {
            State prev = state.getAndSet(State.OPEN);
            if (prev != State.OPEN) {
                totalTrips.incrementAndGet();
                System.err.println("[CircuitBreaker] OPENED after " + failures + " consecutive failures. "
                        + "Will retry in " + resetTimeoutMs + "ms. Evicting stale connections...");
                // Evict stale connections so HALF_OPEN test gets a fresh one
                invokeCallback();
            }
        }
    }

    private void invokeCallback() {
        Runnable cb = onStateChangeCallback;
        if (cb != null) {
            try {
                cb.run();
            } catch (Exception e) {
                System.err.println("[CircuitBreaker] Callback failed: " + e.getMessage());
            }
        }
    }

    public State getState() { return state.get(); }
    public int getFailureCount() { return failureCount.get(); }
    public long getTotalTrips() { return totalTrips.get(); }
}
