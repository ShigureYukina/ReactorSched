package com.yuki.reactorsched.executor.sdk.dto;

import com.yuki.reactorsched.executor.api.ExecutionOutcome;
import com.yuki.reactorsched.executor.api.ErrorCategory;

import java.time.LocalDateTime;

/**
 * Result body of {@code POST /api/v1/executors/{id}/executions/{executionId}/result} (PRD 7.3, 9).
 *
 * @param leaseVersion lease version the attempt was claimed with; a stale one is answered with 409
 *                     and the executor drops the attempt instead of retrying the submission
 * @param outcome      what this executor makes of the attempt
 * @param errorCode    stable code of a failure, e.g. {@code EXECUTION_ERROR}
 * @param errorCategory classification that decides retry versus terminal failure (PRD 5.4)
 * @param message      human-readable failure summary
 * @param log          executor-produced log, already bounded before it was sent (PRD 9)
 * @param logTruncated whether the log was cut at its byte bound
 * @param startedAt    when the handler started
 * @param finishedAt   when the handler returned
 * @param durationMs   wall-clock duration of the attempt
 */
public record ExecutionResultRequest(long leaseVersion,
                                     ExecutionOutcome outcome,
                                     String errorCode,
                                     ErrorCategory errorCategory,
                                     String message,
                                     String log,
                                     boolean logTruncated,
                                     LocalDateTime startedAt,
                                     LocalDateTime finishedAt,
                                     Long durationMs) {
}
