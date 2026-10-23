package com.yuki.reactorsched.executor.registry.exception;

/**
 * Thrown when an executor reports a result or renewal for an attempt that belongs to another
 * executor. The shared executor token authenticates the fleet (PRD 10.3); the attempt's owner is
 * what decides whether this particular process may write it.
 */
public class ExecutorMismatchException extends RuntimeException {

    private final long executionId;

    public ExecutorMismatchException(long executionId) {
        super("execution " + executionId + " belongs to another executor");
        this.executionId = executionId;
    }

    public long getExecutionId() {
        return executionId;
    }
}
