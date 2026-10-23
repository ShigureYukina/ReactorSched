package com.yuki.reactorsched.core.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.JobStatus;
import com.yuki.reactorsched.core.domain.MisfirePolicy;
import com.yuki.reactorsched.core.domain.ScheduleType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Read model of a job definition.
 *
 * <p>Internal bookkeeping that the caller need not depend on (created/updated by, soft delete
 * timestamp) is reduced to what an operator actually needs to make a decision.
 */
public record JobView(
        long id,
        String jobKey,
        String name,
        String description,
        String taskType,
        String executorGroup,
        JsonNode payloadTemplate,
        ScheduleType scheduleType,
        String cronExpr,
        Integer intervalSeconds,
        LocalDateTime runAt,
        MisfirePolicy misfirePolicy,
        int misfireGraceSeconds,
        int shardTotal,
        JobStatus status,
        int priority,
        int maxAttempts,
        long timeoutMs,
        long retryBaseDelayMs,
        BigDecimal retryMultiplier,
        long retryMaxDelayMs,
        BigDecimal retryJitter,
        LocalDateTime nextFireAt,
        LocalDateTime lastFireAt,
        String createdBy,
        String updatedBy,
        LocalDateTime createdAt,
        LocalDateTime updatedAt,
        LocalDateTime deletedAt) {

    public static JobView from(JobDefinition job, ObjectMapper objectMapper) {
        return new JobView(
                job.getId(),
                job.getJobKey(),
                job.getName(),
                job.getDescription(),
                job.getTaskType(),
                job.getExecutorGroup(),
                parseTemplate(job.getPayloadTemplate(), objectMapper),
                job.getScheduleType(),
                job.getCronExpr(),
                job.getIntervalSeconds(),
                job.getRunAt(),
                job.getMisfirePolicy(),
                job.getMisfireGraceSeconds(),
                job.getShardTotal(),
                job.getStatus(),
                job.getPriority(),
                job.getMaxAttempts(),
                job.getTimeoutMs(),
                job.getRetryBaseDelayMs(),
                job.getRetryMultiplier(),
                job.getRetryMaxDelayMs(),
                job.getRetryJitter(),
                job.getNextFireAt(),
                job.getLastFireAt(),
                job.getCreatedBy(),
                job.getUpdatedBy(),
                job.getCreatedAt(),
                job.getUpdatedAt(),
                job.getDeletedAt());
    }

    private static JsonNode parseTemplate(String template, ObjectMapper objectMapper) {
        try {
            return objectMapper.readTree(template);
        } catch (Exception e) {
            // The template is validated on write; this only guards against hand-edited rows.
            return objectMapper.getNodeFactory().textNode(template);
        }
    }
}
