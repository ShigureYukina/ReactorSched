package com.yuki.reactorsched.core.exception;

/**
 * An admission point refused work because the backlog exceeded its threshold (PRD 8.5). The API
 * layer maps this to HTTP 429; the decision row has already been written by the guard.
 */
public class BackpressureRejectedException extends RuntimeException {

    private final String entryPoint;
    private final long dueBacklog;
    private final long waitingQueue;

    public BackpressureRejectedException(String entryPoint, long dueBacklog, long waitingQueue) {
        super(entryPoint + " rejected: due backlog " + dueBacklog + ", waiting queue " + waitingQueue
                + " exceed the configured thresholds (PRD 8.5)");
        this.entryPoint = entryPoint;
        this.dueBacklog = dueBacklog;
        this.waitingQueue = waitingQueue;
    }

    public String getEntryPoint() {
        return entryPoint;
    }

    public long getDueBacklog() {
        return dueBacklog;
    }

    public long getWaitingQueue() {
        return waitingQueue;
    }
}
