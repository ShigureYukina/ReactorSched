package com.yuki.reactorsched.executor.registry.dto;

import com.yuki.reactorsched.executor.api.ExecutionOutcome;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.LocalDateTime;

/**
 * Body of {@code POST /api/v1/executors/{id}/executions/{executionId}/result} (PRD 7.3, 9).
 *
 * @param leaseVersion lease version the attempt was claimed with; a stale one is rejected with 409
 *                     and the attempt is closed as {@code ABANDONED} (PRD 8.2)
 * @param outcome      what the executor makes of the attempt
 * @param errorCode    stable code of a failure, e.g. {@code EXECUTION_ERROR}
 * @param errorCategory classification that decides retry versus terminal failure (PRD 5.4)
 * @param message      human-readable failure summary
 * @param log          executor-produced log, already bounded by the SDK (PRD 9)
 * @param logTruncated whether the SDK cut the log at its byte bound
 * @param startedAt    when the handler started
 * @param finishedAt   when the handler returned
 * @param durationMs   wall-clock duration of the attempt
 */
public record ExecutionResultRequest(
        @Positive(message = "leaseVersion must be positive") long leaseVersion,
        @NotNull(message = "outcome must not be null") ExecutionOutcome outcome,
        String errorCode,
        com.yuki.reactorsched.executor.api.ErrorCategory errorCategory,
        String message,
        String log,
        boolean logTruncated,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        Long durationMs) {
}
