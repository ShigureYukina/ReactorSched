package com.yuki.reactorsched.core.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Persistent task instance (table {@code task}, PRD 11.2).
 *
 * <p>An instance is one planned firing of a job definition, or one direct submission through the
 * compatibility endpoint. It holds a snapshot of everything the executor needs: the payload that was
 * rendered at creation time, the retry budget, the timeout and the executor group. Editing the
 * definition afterwards cannot change an instance that already exists.
 *
 * <p>This is the only place instance state lives: MySQL is the single source of truth (PRD 4.3).
 * Every state change must go through {@link com.yuki.reactorsched.core.statemachine.TaskStateMachine}
 * and a conditional update in {@link com.yuki.reactorsched.core.mapper.TaskMapper}.
 *
 * <p>The table is still named {@code task} and the execution-right column is still
 * {@code fencing_token} in this phase; renaming them to {@code task_instance} / {@code lease_version}
 * touches most of the portfolio and is tracked in {@code docs/REUSE-PLAN.md}.
 */
@Data
@TableName("task")
public class Task {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String taskType;

    /** Raw JSON document; treated as data only, never as code. */
    private String payload;

    private TaskStatus status;

    /** Lower value is scheduled first. */
    private Integer priority;

    /** Only used by the compatibility endpoint (direct submission without a job definition). */
    private String idempotencyScope;

    /** Only used by the compatibility endpoint. */
    private String idempotencyKey;

    /** Plan based identity: {@code job:{id}:{fireEpochMs}:{shard}} or {@code manual:{id}:{clientKey}}. */
    private String identityKey;

    /** Job definition this instance was created from; {@code null} for direct submissions. */
    private Long jobId;

    private Integer shardIndex;

    private Integer shardTotal;

    /** Planned fire time of a scheduled instance; {@code null} for direct submissions. */
    private LocalDateTime scheduledAt;

    /** Executor group allowed to claim this instance. */
    private String executorGroup;

    private Integer attemptCount;

    private Integer maxAttempts;

    private LocalDateTime nextRunAt;

    private Long timeoutMs;

    private String leaseOwner;

    private LocalDateTime leaseUntil;

    /** Incremented on every successful claim, so stale writers can be rejected. */
    private Long fencingToken;

    private String lastError;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}
