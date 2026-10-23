package com.yuki.reactorsched.core.exception;

/**
 * Thrown when an execution attempt does not exist. Distinct from a missing task: an attempt id that
 * was never written is a client error about the attempt, not about the task.
 */
public class ExecutionNotFoundException extends RuntimeException {

    private final long executionId;

    public ExecutionNotFoundException(long executionId) {
        super("execution " + executionId + " does not exist");
        this.executionId = executionId;
    }

    public long getExecutionId() {
        return executionId;
    }
}
