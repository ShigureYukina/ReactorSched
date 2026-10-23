package com.yuki.reactorsched.core.service;

import com.yuki.reactorsched.core.domain.JobStatus;
import com.yuki.reactorsched.core.domain.MisfirePolicy;
import com.yuki.reactorsched.core.domain.ScheduleType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Values supplied by the caller when a job definition is created or edited (PRD 5.2).
 *
 * <p>{@code null} means "leave the current value alone" on an update; on a create every field that
 * has no sensible default must be present, which the validation layer enforces.
 */
public record JobCommand(
        String jobKey,
        String name,
        String description,
        String taskType,
        String executorGroup,
        String payloadTemplate,
        ScheduleType scheduleType,
        String cronExpr,
        Integer intervalSeconds,
        LocalDateTime runAt,
        MisfirePolicy misfirePolicy,
        Integer misfireGraceSeconds,
        Integer shardTotal,
        JobStatus initialStatus,
        Integer priority,
        Integer maxAttempts,
        Long timeoutMs,
        Long retryBaseDelayMs,
        BigDecimal retryMultiplier,
        Long retryMaxDelayMs,
        BigDecimal retryJitter) {
}
