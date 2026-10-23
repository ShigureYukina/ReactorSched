package com.yuki.reactorsched.core.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Persistent job definition (table {@code job_definition}, PRD 5.1).
 *
 * <p>This is configuration, not execution state. Instances are created from it and keep a snapshot
 * of everything they need, so editing or deleting a definition never rewrites what already ran
 * (AGENTS.md 3).
 */
@Data
@TableName("job_definition")
public class JobDefinition {

    /** Longest job key accepted; must stay below the unique index width. */
    public static final int MAX_JOB_KEY_LENGTH = 128;

    @TableId(type = IdType.AUTO)
    private Long id;

    private String jobKey;

    private String name;

    private String description;

    private String taskType;

    private String executorGroup;

    /** JSON template; the placeholders are rendered when an instance is created. */
    private String payloadTemplate;

    private ScheduleType scheduleType;

    /** Six-field cron expression (seconds included), only for {@link ScheduleType#CRON}. */
    private String cronExpr;

    private Integer intervalSeconds;

    private LocalDateTime runAt;

    private MisfirePolicy misfirePolicy;

    private Integer misfireGraceSeconds;

    private Integer shardTotal;

    private JobStatus status;

    private Integer priority;

    private Integer maxAttempts;

    private Long timeoutMs;

    private Long retryBaseDelayMs;

    private BigDecimal retryMultiplier;

    private Long retryMaxDelayMs;

    private BigDecimal retryJitter;

    /** The field schedulers compete for; {@code null} while paused or deleted. */
    private LocalDateTime nextFireAt;

    /** Most recent planned fire time that was claimed and turned into instances. */
    private LocalDateTime lastFireAt;

    private String createdBy;

    private String updatedBy;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    private LocalDateTime deletedAt;
}
