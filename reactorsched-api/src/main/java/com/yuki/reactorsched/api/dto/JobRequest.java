package com.yuki.reactorsched.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.yuki.reactorsched.core.domain.JobStatus;
import com.yuki.reactorsched.core.domain.MisfirePolicy;
import com.yuki.reactorsched.core.domain.ScheduleType;
import com.yuki.reactorsched.core.service.JobCommand;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Request body of {@code POST /api/v1/jobs} and {@code PUT /api/v1/jobs/{id}}.
 *
 * <p>Bounds live on the fields so a bad request is rejected before it reaches the domain, and the
 * rejection carries the field name (PRD 10.2). Cross-field rules that need more than one value - "a
 * cron job must have a cron expression" - are validated by the service, which reports the same
 * {@code details} shape.
 *
 * <p>{@code payloadTemplate} is bound as a JSON node and must be an object; it is stored as data and
 * never interpreted as code (PRD 6).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JobRequest(
        @NotBlank @Size(max = 128, message = "jobKey must not exceed 128 characters") String jobKey,
        @NotBlank @Size(max = 128, message = "name must not exceed 128 characters") String name,
        @Size(max = 512, message = "description must not exceed 512 characters") String description,
        @NotBlank @Size(max = 64, message = "taskType must not exceed 64 characters") String taskType,
        @Size(max = 64, message = "executorGroup must not exceed 64 characters") String executorGroup,
        JsonNode payloadTemplate,
        @NotNull ScheduleType scheduleType,
        @Pattern(regexp = "(?s)^.{0,64}$", message = "cronExpr must not exceed 64 characters") String cronExpr,
        @Min(value = 1, message = "intervalSeconds must be at least 1")
        @Max(value = 2_592_000, message = "intervalSeconds must not exceed 2592000")
        Integer intervalSeconds,
        LocalDateTime runAt,
        MisfirePolicy misfirePolicy,
        @Min(value = 0, message = "misfireGraceSeconds must not be negative")
        @Max(value = 604_800, message = "misfireGraceSeconds must not exceed 604800")
        Integer misfireGraceSeconds,
        @Min(value = 1, message = "shardTotal must be at least 1")
        @Max(value = 1024, message = "shardTotal must not exceed 1024")
        Integer shardTotal,
        JobStatus initialStatus,
        @Min(value = 0, message = "priority must be at least 0")
        @Max(value = 1000, message = "priority must not exceed 1000")
        Integer priority,
        @Min(value = 1, message = "maxAttempts must be at least 1")
        @Max(value = 100, message = "maxAttempts must not exceed 100")
        Integer maxAttempts,
        @Min(value = 100, message = "timeoutMs must be at least 100")
        @Max(value = 86_400_000, message = "timeoutMs must not exceed 86400000")
        Long timeoutMs,
        @Min(value = 1, message = "retryBaseDelayMs must be at least 1")
        Long retryBaseDelayMs,
        @DecimalMin(value = "1.0", message = "retryMultiplier must be at least 1.0")
        @DecimalMax(value = "10.0", message = "retryMultiplier must not exceed 10.0")
        BigDecimal retryMultiplier,
        @Min(value = 1, message = "retryMaxDelayMs must be at least 1")
        Long retryMaxDelayMs,
        @DecimalMin(value = "0.0", message = "retryJitter must be between 0 and 1")
        @DecimalMax(value = "1.0", message = "retryJitter must be between 0 and 1")
        BigDecimal retryJitter) {

    public JobCommand toCommand() {
        return new JobCommand(
                jobKey,
                name,
                description,
                taskType,
                executorGroup,
                payloadTemplate == null ? null : payloadTemplate.toString(),
                scheduleType,
                cronExpr,
                intervalSeconds,
                runAt,
                misfirePolicy,
                misfireGraceSeconds,
                shardTotal,
                initialStatus,
                priority,
                maxAttempts,
                timeoutMs,
                retryBaseDelayMs,
                retryMultiplier,
                retryMaxDelayMs,
                retryJitter);
    }
}
