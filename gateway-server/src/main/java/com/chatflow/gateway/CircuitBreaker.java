package com.chatflow.gateway;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple circuit breaker to prevent cascading failures when RabbitMQ is unavailable.
 * 
 * States:
 *   CLOSED   - Normal operation, requests pass through
 *   OPEN     - Failures exceeded threshold, requests are rejected immediately
 *   HALF_OPEN - After timeout, allow one test request to check if service recovered
 */
public class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);

    private final int failureThreshold;
    private final long resetTimeoutMs;

    /**
     * @param failureThreshold number of consecutive failures before opening the circuit
     * @param resetTimeoutMs   time in ms to wait before trying again (half-open)
     */
    public CircuitBreaker(int failureThreshold, long resetTimeoutMs) {
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
    }

    /**
     * Check if the circuit allows a request to pass through.
     */
    public boolean allowRequest() {
        State current = state.get();

        if (current == State.CLOSED) {
            return true;
        }

        if (current == State.OPEN) {
            // Check if timeout has passed → transition to HALF_OPEN
            if (System.currentTimeMillis() - lastFailureTime.get() > resetTimeoutMs) {
                state.compareAndSet(State.OPEN, State.HALF_OPEN);
                return true; // Allow one test request
            }
            return false;
        }

        // HALF_OPEN: allow the test request
        return true;
    }

    /**
     * Record a successful operation. Resets the circuit to CLOSED.
     */
    public void recordSuccess() {
        failureCount.set(0);
        state.set(State.CLOSED);
    }

    /**
     * Record a failed operation. If threshold exceeded, open the circuit.
     */
    public void recordFailure() {
        int failures = failureCount.incrementAndGet();
        lastFailureTime.set(System.currentTimeMillis());

        if (failures >= failureThreshold) {
            state.set(State.OPEN);
        }
    }

    public State getState() {
        return state.get();
    }

    public int getFailureCount() {
        return failureCount.get();
    }
}
