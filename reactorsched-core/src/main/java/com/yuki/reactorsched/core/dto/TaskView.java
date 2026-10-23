package com.yuki.reactorsched.core.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskStatus;

import java.time.LocalDateTime;

/**
 * Read model of a task instance as returned by the API.
 *
 * <p>Lease ownership and fencing tokens are deliberately absent: they describe internal scheduling
 * mechanics, and exposing them would invite clients to depend on them.
 *
 * <p>The origin fields - {@code jobId}, shard coordinates and {@code scheduledAt} - are present so a
 * caller can tell a planned instance from a direct submission and identify which shard it is.
 */
public record TaskView(
        long id,
        Long jobId,
        String identityKey,
        int shardIndex,
        int shardTotal,
        LocalDateTime scheduledAt,
        String executorGroup,
        String taskType,
        JsonNode payload,
        TaskStatus status,
        int priority,
        String idempotencyScope,
        String idempotencyKey,
        int attemptCount,
        int maxAttempts,
        LocalDateTime nextRunAt,
        long timeoutMs,
        String lastError,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {

    public static TaskView from(Task task, ObjectMapper objectMapper) {
        return new TaskView(
                task.getId(),
                task.getJobId(),
                task.getIdentityKey(),
                task.getShardIndex() == null ? 0 : task.getShardIndex(),
                task.getShardTotal() == null ? 1 : task.getShardTotal(),
                task.getScheduledAt(),
                task.getExecutorGroup(),
                task.getTaskType(),
                parsePayload(task.getPayload(), objectMapper),
                task.getStatus(),
                task.getPriority(),
                task.getIdempotencyScope(),
                task.getIdempotencyKey(),
                task.getAttemptCount(),
                task.getMaxAttempts(),
                task.getNextRunAt(),
                task.getTimeoutMs(),
                task.getLastError(),
                task.getCreatedAt(),
                task.getUpdatedAt());
    }

    private static JsonNode parsePayload(String payload, ObjectMapper objectMapper) {
        try {
            return objectMapper.readTree(payload);
        } catch (Exception e) {
            // The payload was validated at submission time; this only guards against hand-edited rows.
            return objectMapper.getNodeFactory().textNode(payload);
        }
    }
}
