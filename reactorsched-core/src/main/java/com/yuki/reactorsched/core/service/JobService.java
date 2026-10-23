package com.yuki.reactorsched.core.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.core.decision.SchedulerDecisionRecorder;
import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.ExecutorGroups;
import com.yuki.reactorsched.core.domain.JobStatus;
import com.yuki.reactorsched.core.domain.MisfirePolicy;
import com.yuki.reactorsched.core.domain.ScheduleType;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.core.dto.JobPage;
import com.yuki.reactorsched.core.dto.JobQuery;
import com.yuki.reactorsched.core.dto.JobView;
import com.yuki.reactorsched.core.exception.InvalidJobStateException;
import com.yuki.reactorsched.core.exception.JobNotFoundException;
import com.yuki.reactorsched.executor.api.TaskHandlerRegistry;
import com.yuki.reactorsched.core.statemachine.JobStateMachine;
import com.yuki.reactorsched.core.mapper.JobDefinitionMapper;
import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.core.schedule.ScheduleCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Lifecycle of job definitions (PRD 5).
 *
 * <p>Every state change goes through {@link com.yuki.reactorsched.core.statemachine.JobStateMachine}
 * first and is then applied by a conditional update that repeats the expected state in its
 * {@code WHERE} clause. Two operators, or an operator racing the scheduler, can therefore never both
 * succeed on one row.
 *
 * <p>Instance creation is not in this class: {@link InstanceFactory} owns it, so that the "what to
 * run" rule and the "how often" rule stay readable apart.
 */
@Profile("!executor")
@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    static final String DEFAULT_EXECUTOR_GROUP = ExecutorGroups.DEFAULT;
    static final int DEFAULT_PRIORITY = 100;
    static final int DEFAULT_MAX_ATTEMPTS = 3;
    static final long DEFAULT_TIMEOUT_MS = 60_000L;
    static final int DEFAULT_MISFIRE_GRACE_SECONDS = 300;
    static final int DEFAULT_SHARD_TOTAL = 1;
    static final long DEFAULT_RETRY_BASE_DELAY_MS = 1_000L;
    static final BigDecimal DEFAULT_RETRY_MULTIPLIER = new BigDecimal("2.00");
    static final long DEFAULT_RETRY_MAX_DELAY_MS = 300_000L;
    static final BigDecimal DEFAULT_RETRY_JITTER = new BigDecimal("0.20");

    private static final int MIN_PRIORITY = 0;
    private static final int MAX_PRIORITY = 1000;
    private static final int MAX_MAX_ATTEMPTS = 100;
    private static final long MIN_TIMEOUT_MS = 100L;
    private static final long MAX_TIMEOUT_MS = 86_400_000L;
    private static final int MAX_TASK_TYPE_LENGTH = 64;
    private static final int MAX_GROUP_LENGTH = 64;
    private static final int MAX_NAME_LENGTH = 128;
    private static final int MAX_DESCRIPTION_LENGTH = 512;
    private static final int MAX_CRON_LENGTH = 64;
    private static final int MIN_INTERVAL_SECONDS = 1;
    private static final int MAX_INTERVAL_SECONDS = 2_592_000;
    private static final int MIN_MISFIRE_GRACE = 0;
    private static final int MAX_MISFIRE_GRACE = 604_800;
    private static final int MAX_SHARD_TOTAL = 1024;
    private static final int MAX_OPERATOR_LENGTH = 64;
    private static final int MAX_REASON_LENGTH = 512;
    private static final int MAX_IDENTITY_CLIENT_KEY_LENGTH = 128;

    private final JobDefinitionMapper jobMapper;
    private final TaskMapper taskMapper;
    private final TaskHandlerRegistry handlerRegistry;
    private final SchedulerDecisionRecorder decisionRecorder;
    private final InstanceFactory instanceFactory;
    private final OperationAuditService auditService;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public JobService(JobDefinitionMapper jobMapper,
                      TaskMapper taskMapper,
                      TaskHandlerRegistry handlerRegistry,
                      SchedulerDecisionRecorder decisionRecorder,
                      InstanceFactory instanceFactory,
                      OperationAuditService auditService,
                      ObjectMapper objectMapper,
                      Clock clock) {
        this.jobMapper = jobMapper;
        this.taskMapper = taskMapper;
        this.handlerRegistry = handlerRegistry;
        this.decisionRecorder = decisionRecorder;
        this.instanceFactory = instanceFactory;
        this.auditService = auditService;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /**
     * Creates a definition. The job key is unique in the database, so a duplicate is reported instead
     * of silently overwriting a definition that may already have instances.
     */
    @Transactional
    public JobDefinition create(JobCommand command, String operator) {
        String actor = requireOperator(operator);
        String jobKey = requireJobKey(command.jobKey());
        validateSchedule(command);

        LocalDateTime now = LocalDateTime.now(clock);
        JobDefinition job = new JobDefinition();
        job.setJobKey(jobKey);
        job.setName(requireName(command.name()));
        job.setDescription(normalizeDescription(command.description()));
        job.setTaskType(requireTaskType(command.taskType()));
        job.setExecutorGroup(normalizeGroup(command.executorGroup()));
        job.setPayloadTemplate(normalizeTemplate(command.payloadTemplate()));
        job.setScheduleType(requireScheduleType(command.scheduleType()));
        job.setCronExpr(command.cronExpr() == null ? null : command.cronExpr().trim());
        job.setIntervalSeconds(command.intervalSeconds());
        job.setRunAt(command.runAt());
        job.setMisfirePolicy(command.misfirePolicy() == null ? MisfirePolicy.SKIP : command.misfirePolicy());
        job.setMisfireGraceSeconds(command.misfireGraceSeconds() == null
                ? DEFAULT_MISFIRE_GRACE_SECONDS : command.misfireGraceSeconds());
        job.setShardTotal(command.shardTotal() == null ? DEFAULT_SHARD_TOTAL : command.shardTotal());
        JobStatus status = command.initialStatus() == null ? JobStatus.ENABLED : command.initialStatus();
        job.setStatus(status);
        job.setPriority(command.priority() == null ? DEFAULT_PRIORITY : command.priority());
        job.setMaxAttempts(command.maxAttempts() == null ? DEFAULT_MAX_ATTEMPTS : command.maxAttempts());
        job.setTimeoutMs(command.timeoutMs() == null ? DEFAULT_TIMEOUT_MS : command.timeoutMs());
        job.setRetryBaseDelayMs(command.retryBaseDelayMs() == null
                ? DEFAULT_RETRY_BASE_DELAY_MS : command.retryBaseDelayMs());
        job.setRetryMultiplier(command.retryMultiplier() == null
                ? DEFAULT_RETRY_MULTIPLIER : command.retryMultiplier());
        job.setRetryMaxDelayMs(command.retryMaxDelayMs() == null
                ? DEFAULT_RETRY_MAX_DELAY_MS : command.retryMaxDelayMs());
        job.setRetryJitter(command.retryJitter() == null ? DEFAULT_RETRY_JITTER : command.retryJitter());
        job.setCreatedBy(actor);
        job.setUpdatedBy(actor);
        job.setCreatedAt(now);
        job.setUpdatedAt(now);
        job.setNextFireAt(status == JobStatus.ENABLED
                ? ScheduleCalculator.firstFireAtOrAfter(job, earliestStart(job, now))
                : null);

        try {
            jobMapper.insert(job);
        } catch (DuplicateKeyException e) {
            throw new InvalidJobStateException("job key '" + jobKey + "' already exists");
        }
        log.info("job {} created by {}: type={} schedule={} shards={} nextFireAt={}",
                job.getId(), actor, job.getTaskType(), job.getScheduleType(), job.getShardTotal(),
                job.getNextFireAt());
        auditService.record(actor, OperationAuditService.ACTION_CREATE, OperationAuditService.TARGET_JOB,
                job.getId(), "created through the job API", null, JobView.from(job, objectMapper));
        return job;
    }

    public JobDefinition get(long jobId) {
        return requireJob(jobId);
    }

    /**
     * Applies a partial edit. Fields that are {@code null} in the command keep their current value.
     * Editing a schedule recomputes {@code next_fire_at} with the observed value in the
     * {@code WHERE} clause, so an edit and a scheduler claiming the same fire time cannot both win.
     */
    @Transactional
    public JobDefinition update(long jobId, JobCommand command, String operator) {
        String actor = requireOperator(operator);
        JobDefinition current = requireJob(jobId);
        if (current.getStatus() == JobStatus.DELETED) {
            throw new InvalidJobStateException("job " + jobId + " is deleted and cannot be edited");
        }

        JobCommand merged = merge(current, command);
        validateSchedule(merged);

        JobDefinition updated = new JobDefinition();
        updated.setId(jobId);
        updated.setId(current.getId());
        updated.setJobKey(current.getJobKey());
        updated.setName(requireName(merged.name()));
        updated.setDescription(normalizeDescription(merged.description()));
        updated.setTaskType(requireTaskType(merged.taskType()));
        updated.setExecutorGroup(normalizeGroup(merged.executorGroup()));
        updated.setPayloadTemplate(normalizeTemplate(merged.payloadTemplate()));
        updated.setScheduleType(requireScheduleType(merged.scheduleType()));
        updated.setCronExpr(merged.cronExpr() == null ? null : merged.cronExpr().trim());
        updated.setIntervalSeconds(merged.intervalSeconds());
        updated.setRunAt(merged.runAt());
        updated.setMisfirePolicy(merged.misfirePolicy() == null ? MisfirePolicy.SKIP : merged.misfirePolicy());
        updated.setMisfireGraceSeconds(merged.misfireGraceSeconds() == null
                ? DEFAULT_MISFIRE_GRACE_SECONDS : merged.misfireGraceSeconds());
        updated.setShardTotal(merged.shardTotal() == null ? DEFAULT_SHARD_TOTAL : merged.shardTotal());
        updated.setPriority(merged.priority() == null ? DEFAULT_PRIORITY : merged.priority());
        updated.setMaxAttempts(merged.maxAttempts() == null ? DEFAULT_MAX_ATTEMPTS : merged.maxAttempts());
        updated.setTimeoutMs(merged.timeoutMs() == null ? DEFAULT_TIMEOUT_MS : merged.timeoutMs());
        updated.setRetryBaseDelayMs(merged.retryBaseDelayMs() == null
                ? DEFAULT_RETRY_BASE_DELAY_MS : merged.retryBaseDelayMs());
        updated.setRetryMultiplier(merged.retryMultiplier() == null
                ? DEFAULT_RETRY_MULTIPLIER : merged.retryMultiplier());
        updated.setRetryMaxDelayMs(merged.retryMaxDelayMs() == null
                ? DEFAULT_RETRY_MAX_DELAY_MS : merged.retryMaxDelayMs());
        updated.setRetryJitter(merged.retryJitter() == null ? DEFAULT_RETRY_JITTER : merged.retryJitter());
        updated.setUpdatedBy(actor);

        // The bookkeeping columns are written by the conditional statement below, not by this entity.
        updated.setStatus(current.getStatus());
        updated.setNextFireAt(current.getNextFireAt());
        updated.setLastFireAt(current.getLastFireAt());
        updated.setCreatedBy(current.getCreatedBy());
        updated.setCreatedAt(current.getCreatedAt());
        jobMapper.updateById(updated);

        boolean scheduleChanged = scheduleChanged(current, updated);
        if (current.getStatus() == JobStatus.ENABLED || scheduleChanged) {
            LocalDateTime now = LocalDateTime.now(clock);
            LocalDateTime next = current.getStatus() == JobStatus.ENABLED
                    ? ScheduleCalculator.firstFireAtOrAfter(updated, now)
                    : null;
            int applied = jobMapper.reschedule(jobId, current.getStatus(), current.getNextFireAt(), next, actor, now);
            if (applied != 1) {
                // Someone else advanced the fire time between the read and the write; re-read so the
                // caller sees the state that actually won instead of a half-applied view.
                log.info("job {} was rescheduled concurrently; returning the stored state", jobId);
            }
        }
        JobDefinition after = get(jobId);
        log.info("job {} edited by {}: scheduleChanged={} nextFireAt={}",
                jobId, actor, scheduleChanged, after.getNextFireAt());
        auditService.record(actor, OperationAuditService.ACTION_UPDATE, OperationAuditService.TARGET_JOB, jobId,
                "edited through the job API", current, JobView.from(after, objectMapper));
        return after;
    }

    /**
     * Pauses a definition: it stops producing instances and loses its planned fire time. Pausing an
     * already paused job is rejected by the state machine rather than silently repeated.
     */
    @Transactional
    public JobDefinition pause(long jobId, String operator, String reason) {
        String actor = requireOperator(operator);
        JobDefinition current = requireJob(jobId);
        JobStateMachine.requireTransition(current.getStatus(), JobStatus.PAUSED);
        LocalDateTime now = LocalDateTime.now(clock);
        int applied = jobMapper.changeStatus(jobId, current.getStatus(), JobStatus.PAUSED, null, actor, now);
        if (applied != 1) {
            throw new InvalidJobStateException("job " + jobId + " changed state while it was being paused");
        }
        log.info("job {} paused by {}", jobId, actor);
        JobDefinition paused = get(jobId);
        auditService.record(actor, OperationAuditService.ACTION_PAUSE, OperationAuditService.TARGET_JOB, jobId,
                requireReason(reason), current, JobView.from(paused, objectMapper));
        return paused;
    }

    /**
     * Resumes a paused definition. The planned fire time is recomputed from now: fire times that
     * passed while the job was paused are not compensated (PRD 4.2).
     */
    @Transactional
    public JobDefinition enable(long jobId, String operator, String reason) {
        String actor = requireOperator(operator);
        JobDefinition current = requireJob(jobId);
        JobStateMachine.requireTransition(current.getStatus(), JobStatus.ENABLED);
        LocalDateTime now = LocalDateTime.now(clock);
        LocalDateTime next = ScheduleCalculator.firstFireAtOrAfter(current, now);
        int applied = jobMapper.changeStatus(jobId, current.getStatus(), JobStatus.ENABLED, next, actor, now);
        if (applied != 1) {
            throw new InvalidJobStateException("job " + jobId + " changed state while it was being enabled");
        }
        log.info("job {} enabled by {}: nextFireAt={}", jobId, actor, next);
        JobDefinition enabled = get(jobId);
        auditService.record(actor, OperationAuditService.ACTION_ENABLE, OperationAuditService.TARGET_JOB, jobId,
                requireReason(reason), current, JobView.from(enabled, objectMapper));
        return enabled;
    }

    /**
     * Soft deletes a definition. Instances that were already created keep running and stay queryable;
     * the job key stays taken so history can never point at a different definition later.
     */
    @Transactional
    public JobDefinition delete(long jobId, String operator, String reason) {
        String actor = requireOperator(operator);
        JobDefinition current = requireJob(jobId);
        JobStateMachine.requireTransition(current.getStatus(), JobStatus.DELETED);
        int applied = jobMapper.softDelete(jobId, actor, LocalDateTime.now(clock));
        if (applied != 1) {
            throw new InvalidJobStateException("job " + jobId + " changed state while it was being deleted");
        }
        log.info("job {} deleted by {}", jobId, actor);
        JobDefinition deleted = get(jobId);
        auditService.record(actor, OperationAuditService.ACTION_DELETE, OperationAuditService.TARGET_JOB, jobId,
                requireReason(reason), current, JobView.from(deleted, objectMapper));
        return deleted;
    }

    /** One page of definitions; paging bounds are validated by the API layer. */
    public JobPage query(JobQuery query) {
        long total = jobMapper.countByQuery(query.status(), query.from(), query.to());
        long offset = (long) query.page() * query.size();
        List<JobView> items = jobMapper.findPage(query.status(), query.from(), query.to(), query.size(), offset)
                .stream()
                .map(job -> JobView.from(job, objectMapper))
                .toList();
        return new JobPage(items, total, query.page(), query.size());
    }

    /** Definition times are planned in the future; a one-off in the past is reported, not silently dropped. */
    private static LocalDateTime earliestStart(JobDefinition job, LocalDateTime now) {
        return switch (job.getScheduleType()) {
            case ONE_TIME -> {
                if (job.getRunAt() == null) {
                    throw new IllegalArgumentException("runAt is required for ONE_TIME schedules");
                }
                if (job.getRunAt().isBefore(now)) {
                    throw new IllegalArgumentException("runAt must not be in the past");
                }
                yield job.getRunAt();
            }
            case FIXED_RATE -> job.getRunAt() != null && job.getRunAt().isAfter(now) ? job.getRunAt() : now;
            case CRON -> now;
        };
    }

    private static void validateSchedule(JobCommand command) {
        ScheduleType type = requireScheduleType(command.scheduleType());
        switch (type) {
            case CRON -> {
                if (command.cronExpr() == null || command.cronExpr().isBlank()) {
                    throw new IllegalArgumentException("cronExpr is required for CRON schedules");
                }
                if (command.cronExpr().trim().length() > MAX_CRON_LENGTH) {
                    throw new IllegalArgumentException("cronExpr must not exceed " + MAX_CRON_LENGTH + " characters");
                }
                ScheduleCalculator.parseCron(command.cronExpr());
            }
            case FIXED_RATE -> {
                Integer seconds = command.intervalSeconds();
                if (seconds == null) {
                    throw new IllegalArgumentException("intervalSeconds is required for FIXED_RATE schedules");
                }
                if (seconds < MIN_INTERVAL_SECONDS || seconds > MAX_INTERVAL_SECONDS) {
                    throw new IllegalArgumentException("intervalSeconds must be between "
                            + MIN_INTERVAL_SECONDS + " and " + MAX_INTERVAL_SECONDS);
                }
            }
            case ONE_TIME -> {
                if (command.runAt() == null) {
                    throw new IllegalArgumentException("runAt is required for ONE_TIME schedules");
                }
            }
        }
    }

    private static JobCommand merge(JobDefinition current, JobCommand command) {
        return new JobCommand(
                current.getJobKey(),
                command.name() == null ? current.getName() : command.name(),
                command.description() == null ? current.getDescription() : command.description(),
                command.taskType() == null ? current.getTaskType() : command.taskType(),
                command.executorGroup() == null ? current.getExecutorGroup() : command.executorGroup(),
                command.payloadTemplate() == null ? current.getPayloadTemplate() : command.payloadTemplate(),
                command.scheduleType() == null ? current.getScheduleType() : command.scheduleType(),
                command.cronExpr() == null ? current.getCronExpr() : command.cronExpr(),
                command.intervalSeconds() == null ? current.getIntervalSeconds() : command.intervalSeconds(),
                command.runAt() == null ? current.getRunAt() : command.runAt(),
                command.misfirePolicy() == null ? current.getMisfirePolicy() : command.misfirePolicy(),
                command.misfireGraceSeconds() == null ? current.getMisfireGraceSeconds() : command.misfireGraceSeconds(),
                command.shardTotal() == null ? current.getShardTotal() : command.shardTotal(),
                current.getStatus(),
                command.priority() == null ? current.getPriority() : command.priority(),
                command.maxAttempts() == null ? current.getMaxAttempts() : command.maxAttempts(),
                command.timeoutMs() == null ? current.getTimeoutMs() : command.timeoutMs(),
                command.retryBaseDelayMs() == null ? current.getRetryBaseDelayMs() : command.retryBaseDelayMs(),
                command.retryMultiplier() == null ? current.getRetryMultiplier() : command.retryMultiplier(),
                command.retryMaxDelayMs() == null ? current.getRetryMaxDelayMs() : command.retryMaxDelayMs(),
                command.retryJitter() == null ? current.getRetryJitter() : command.retryJitter());
    }

    private static boolean scheduleChanged(JobDefinition before, JobDefinition after) {
        return before.getScheduleType() != after.getScheduleType()
                || !Objects.equals(before.getCronExpr(), after.getCronExpr())
                || !Objects.equals(before.getIntervalSeconds(), after.getIntervalSeconds())
                || !Objects.equals(before.getRunAt(), after.getRunAt());
    }

    private JobDefinition requireJob(long jobId) {
        JobDefinition job = jobMapper.selectById(jobId);
        if (job == null) {
            throw new JobNotFoundException(jobId);
        }
        return job;
    }

    private String requireJobKey(String jobKey) {
        if (jobKey == null || jobKey.isBlank()) {
            throw new IllegalArgumentException("jobKey is required");
        }
        String trimmed = jobKey.trim();
        if (trimmed.length() > JobDefinition.MAX_JOB_KEY_LENGTH) {
            throw new IllegalArgumentException("jobKey must not exceed " + JobDefinition.MAX_JOB_KEY_LENGTH + " characters");
        }
        return trimmed;
    }

    private String requireName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name is required");
        }
        String trimmed = name.trim();
        if (trimmed.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("name must not exceed " + MAX_NAME_LENGTH + " characters");
        }
        return trimmed;
    }

    private static String normalizeDescription(String description) {
        if (description == null) {
            return null;
        }
        String trimmed = description.trim();
        if (trimmed.length() > MAX_DESCRIPTION_LENGTH) {
            throw new IllegalArgumentException("description must not exceed " + MAX_DESCRIPTION_LENGTH + " characters");
        }
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String requireTaskType(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("taskType is required");
        }
        String trimmed = taskType.trim();
        if (trimmed.length() > MAX_TASK_TYPE_LENGTH) {
            throw new IllegalArgumentException("taskType must not exceed " + MAX_TASK_TYPE_LENGTH + " characters");
        }
        if (!handlerRegistry.knows(trimmed)) {
            throw new com.yuki.reactorsched.executor.api.UnknownTaskTypeException(trimmed);
        }
        return trimmed;
    }

    private static String normalizeGroup(String group) {
        if (group == null || group.isBlank()) {
            return DEFAULT_EXECUTOR_GROUP;
        }
        String trimmed = group.trim();
        if (trimmed.length() > MAX_GROUP_LENGTH) {
            throw new IllegalArgumentException("executorGroup must not exceed " + MAX_GROUP_LENGTH + " characters");
        }
        return trimmed;
    }

    /**
     * Payload templates are accepted as JSON objects only, and are stored as data. They are never
     * interpreted as a class name, SQL fragment or script (PRD 6, AGENTS.md 6).
     */
    private String normalizeTemplate(String template) {
        String candidate = (template == null || template.isBlank()) ? "{}" : template;
        JsonNode node;
        try {
            node = objectMapper.readTree(candidate);
        } catch (Exception e) {
            throw new IllegalArgumentException("payloadTemplate must be valid JSON: " + e.getMessage());
        }
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("payloadTemplate must be a JSON object");
        }
        return node.toString();
    }

    private static ScheduleType requireScheduleType(ScheduleType type) {
        if (type == null) {
            throw new IllegalArgumentException("scheduleType is required");
        }
        return type;
    }

    private static String requireOperator(String operator) {
        if (operator == null || operator.isBlank()) {
            throw new IllegalArgumentException("operator is required for administrative operations");
        }
        String trimmed = operator.trim();
        if (trimmed.length() > MAX_OPERATOR_LENGTH) {
            throw new IllegalArgumentException("operator must not exceed " + MAX_OPERATOR_LENGTH + " characters");
        }
        return trimmed;
    }

    /**
     * The operator-supplied reason is what lands in {@code operation_audit}: a row that says
     * "paused through the job API" instead of why the operator paused it explains nothing later.
     */
    private static String requireReason(String reason) {
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason is required for administrative operations");
        }
        String trimmed = reason.trim();
        if (trimmed.length() > MAX_REASON_LENGTH) {
            throw new IllegalArgumentException("reason must not exceed " + MAX_REASON_LENGTH + " characters");
        }
        return trimmed;
    }

    /** Bounds applied to the numeric fields; exposed for the validation layer and for tests. */
    static Map<String, int[]> numericBounds() {
        return Map.of(
                "priority", new int[]{MIN_PRIORITY, MAX_PRIORITY},
                "maxAttempts", new int[]{1, MAX_MAX_ATTEMPTS},
                "shardTotal", new int[]{1, MAX_SHARD_TOTAL},
                "misfireGraceSeconds", new int[]{MIN_MISFIRE_GRACE, MAX_MISFIRE_GRACE});
    }

    static long minTimeoutMs() {
        return MIN_TIMEOUT_MS;
    }

    static long maxTimeoutMs() {
        return MAX_TIMEOUT_MS;
    }

    public List<JobDefinition> dueJobs(LocalDateTime now, int limit) {
        return new ArrayList<>(jobMapper.findDue(now, limit));
    }

    /** Instances created for a definition, newest first. Used by the job detail view. */
    public List<Task> instances(long jobId, int limit) {
        requireJob(jobId);
        return taskMapper.selectList(new com.baomidou.mybatisplus.core.conditions.query.QueryWrapper<Task>()
                .eq("job_id", jobId)
                .orderByDesc("id")
                .last("LIMIT " + Math.max(1, Math.min(limit, 500))));
    }

    /**
     * Triggers a definition once, outside its schedule (PRD 5.2).
     *
     * <p>Allowed on a paused job too - "run it now exactly once" is a different intent from "make it
     * scheduled again" - but every trigger is audited. The client key makes a retried request
     * idempotent: the same key returns the instance it created the first time.
     */
    @Transactional
    public ManualTrigger trigger(long jobId, String clientKey, String operator, String reason) {
        String actor = requireOperator(operator);
        if (clientKey == null || clientKey.isBlank()) {
            throw new IllegalArgumentException("clientKey is required for a manual trigger");
        }
        String key = clientKey.trim();
        if (key.length() > MAX_IDENTITY_CLIENT_KEY_LENGTH) {
            throw new IllegalArgumentException("clientKey must not exceed "
                    + MAX_IDENTITY_CLIENT_KEY_LENGTH + " characters");
        }
        JobDefinition job = requireJob(jobId);
        if (job.getStatus() == JobStatus.DELETED) {
            throw new InvalidJobStateException("job " + jobId + " is deleted and cannot be triggered");
        }
        InstanceFactory.ManualInstance created = instanceFactory.createManualInstance(job, key, actor);
        if (created.created()) {
            auditService.record(actor, OperationAuditService.ACTION_TRIGGER, OperationAuditService.TARGET_JOB, jobId,
                    reason, null, Map.of("instanceId", created.instance().getId(), "clientKey", key));
        }
        return new ManualTrigger(created.instance(), created.created());
    }

    /** The instance a manual trigger produced, and whether this call is what created it. */
    public record ManualTrigger(Task instance, boolean created) {
    }

    /**
     * Records that a claimed fire time could not be turned into instances. The next fire time has
     * already been advanced by the claim, so nothing is retried here: the fire time is simply lost and
     * has to be explainable (PRD 6.2).
     */
    public void recordTriggerFailure(JobDefinition job, LocalDateTime plannedFireAt, Throwable cause) {
        decisionRecorder.recordJobTriggerFailure(job.getId(), plannedFireAt,
                cause == null ? "unknown" : cause.getMessage(), LocalDateTime.now(clock));
    }
}
