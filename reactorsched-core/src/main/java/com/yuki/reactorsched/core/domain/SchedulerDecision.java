package com.yuki.reactorsched.core.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * One scheduling or control decision, stored for later explanation (PRD 8.7).
 *
 * <p>Only real decisions are stored, not every metric sample: the table is audit data and must not
 * become the place where high frequency metrics pile up. Task level fields stay {@code null} for
 * process level decisions such as a concurrency adjustment, and job level fields stay {@code null}
 * for decisions that are about a single instance.
 */
@Data
@TableName("scheduler_decision")
public class SchedulerDecision {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long taskId;

    /** Job definition a job level decision is about; {@code null} for instance level decisions. */
    private Long jobId;

    /** Shard a shard level decision is about; {@code null} when the decision is not shard specific. */
    private Integer shardIndex;

    /** Attempt the decision is about; {@code null} for job and process level decisions. */
    private Long executionId;

    private DecisionType decisionType;

    private String reasonCode;

    private String workerId;

    /** Concurrency before an adjustment; only written by {@link DecisionType#CONCURRENCY_ADJUSTMENT}. */
    private Integer concurrencyBefore;

    /** Concurrency after an adjustment; only written by {@link DecisionType#CONCURRENCY_ADJUSTMENT}. */
    private Integer concurrencyAfter;

    /** Concurrency in force when the decision was taken, for decisions that do not change it. */
    private Integer concurrencyLimit;

    /** Retry time the decision chose, written by {@link ReasonCodes#RETRY_BACKOFF}. */
    private LocalDateTime nextRunAt;

    private Integer runningTasks;

    private Integer backlog;

    private Double downstreamLatencyMs;

    private Double downstreamErrorRate;

    private String observedDetails;

    private LocalDateTime createdAt;
}
