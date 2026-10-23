package com.yuki.reactorsched.executor.sdk.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDateTime;

/**
 * One instance the center handed to this executor (PRD 7.2).
 *
 * <p>Everything needed to execute and to report back travels with the claim, so the executor never
 * reads the database itself (AGENTS.md 6).
 *
 * @param taskId       logical instance id
 * @param executionId  attempt record opened for this claim
 * @param attempt      attempt number of this execution, starting at 1
 * @param fencingToken lease version the result must carry (PRD 8.2)
 * @param taskType     registered handler type
 * @param payload      rendered payload snapshot; data only, never interpreted as code
 * @param shardIndex   zero-based shard inside the planned fire time
 * @param shardTotal   number of shards the planned fire time was expanded into
 * @param scheduledAt  planned fire time this instance belongs to
 * @param timeoutMs    execution timeout; the watchdog is cooperative (PRD 8.3)
 * @param leaseUntil   when the lease runs out unless it is renewed
 */
public record ClaimedTask(long taskId,
                          long executionId,
                          int attempt,
                          long fencingToken,
                          String taskType,
                          JsonNode payload,
                          int shardIndex,
                          int shardTotal,
                          LocalDateTime scheduledAt,
                          Long timeoutMs,
                          LocalDateTime leaseUntil) {
}
