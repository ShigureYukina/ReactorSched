package com.yuki.reactorsched.executor.registry.exception;

/** Thrown when an executor that is not ONLINE tries to claim tasks (PRD 7.1). */
public class ExecutorOfflineException extends RuntimeException {

    private final long executorId;

    public ExecutorOfflineException(long executorId) {
        super("executor " + executorId + " is not ONLINE and cannot claim tasks");
        this.executorId = executorId;
    }

    public long getExecutorId() {
        return executorId;
    }
}
