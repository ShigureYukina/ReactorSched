package com.yuki.reactorsched.core.dto;

import com.yuki.reactorsched.core.domain.ExecutionResult;
import com.yuki.reactorsched.core.domain.TaskExecution;
import com.yuki.reactorsched.executor.api.ErrorCategory;

import java.time.LocalDateTime;

/**
 * Read model of one execution attempt (PRD 7.2). {@code result} is {@code null} while the attempt
 * is still running; {@code errorCategory} classifies a failure (PRD 5.4); {@code logText} is the
 * executor-reported log, bounded before it was sent (PRD 9).
 */
public record TaskExecutionView(
        long id,
        long taskId,
        int attempt,
        String workerId,
        long fencingToken,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        ExecutionResult result,
        String errorCode,
        ErrorCategory errorCategory,
        String errorMessage,
        String logText,
        boolean logTruncated) {

    public static TaskExecutionView from(TaskExecution execution) {
        return new TaskExecutionView(
                execution.getId(),
                execution.getTaskId(),
                execution.getAttempt(),
                execution.getWorkerId(),
                execution.getFencingToken(),
                execution.getStartedAt(),
                execution.getFinishedAt(),
                execution.getResult(),
                execution.getErrorCode(),
                execution.getErrorCategory(),
                execution.getErrorMessage(),
                execution.getLogText(),
                Boolean.TRUE.equals(execution.getLogTruncated()));
    }
}
