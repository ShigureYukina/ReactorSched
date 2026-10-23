package com.yuki.reactorsched.executor.registry.dto;

import com.yuki.reactorsched.executor.registry.domain.ExecutorStatus;

import java.time.LocalDateTime;

/**
 * Read model of one registered executor (PRD 7.1). Returned by the open {@code GET /executors}.
 */
public record ExecutorView(long id,
                           String appName,
                           String instanceId,
                           String groupName,
                           String address,
                           ExecutorStatus status,
                           LocalDateTime lastHeartbeatAt,
                           LocalDateTime registeredAt,
                           String metadata) {

    public static ExecutorView from(com.yuki.reactorsched.executor.registry.domain.Executor executor) {
        return new ExecutorView(executor.getId(), executor.getAppName(), executor.getInstanceId(),
                executor.getGroupName(), executor.getAddress(), executor.getStatus(),
                executor.getLastHeartbeatAt(), executor.getRegisteredAt(), executor.getMetadata());
    }
}
