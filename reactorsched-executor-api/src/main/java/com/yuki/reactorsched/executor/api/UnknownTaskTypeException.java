package com.yuki.reactorsched.executor.api;

/**
 * Thrown when a task type has no registered handler. Lives next to the SPI so an executor process
 * fails with the same error the scheduling center would have produced.
 */
public class UnknownTaskTypeException extends RuntimeException {

    private final String taskType;

    public UnknownTaskTypeException(String taskType) {
        super("no task handler is registered for task type '" + taskType + "'");
        this.taskType = taskType;
    }

    public String getTaskType() {
        return taskType;
    }
}
