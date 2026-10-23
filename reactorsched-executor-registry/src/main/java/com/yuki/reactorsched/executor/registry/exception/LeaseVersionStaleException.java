package com.yuki.reactorsched.executor.registry.exception;

/**
 * Thrown when a result or renewal arrives with a lease version that no longer owns the instance
 * (PRD 8.2). The attempt has already been closed as {@code ABANDONED} with
 * {@code STALE_WRITER}; the executor records a warning and does not retry the submission.
 */
public class LeaseVersionStaleException extends RuntimeException {

    private final long executionId;

    public LeaseVersionStaleException(long executionId) {
        super("execution " + executionId + " no longer owns its lease version");
        this.executionId = executionId;
    }

    public long getExecutionId() {
        return executionId;
    }
}
