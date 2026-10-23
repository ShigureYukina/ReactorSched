package com.yuki.reactorsched.executor.registry.exception;

/** Thrown when an executor id does not exist in the registry. */
public class ExecutorNotFoundException extends RuntimeException {

    private final long executorId;

    public ExecutorNotFoundException(long executorId) {
        super("executor " + executorId + " is not registered");
        this.executorId = executorId;
    }

    public long getExecutorId() {
        return executorId;
    }
}
