package com.yuki.reactorsched.core.service;

import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.JobStatus;
import com.yuki.reactorsched.core.domain.ScheduleType;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.core.schedule.ScheduleCalculator;
import com.yuki.reactorsched.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns one planned fire time of a job definition into instances (PRD 6.3, 5.3).
 *
 * <p>Every instance carries an {@code identity_key} of the form
 * {@code job:{jobId}:{fireTimeEpochMs}:{shardIndex}} and is written with a single idempotent insert.
 * Two schedulers, a retried trigger, or a catch-up that overlaps the previous run therefore converge
 * on the same rows instead of creating duplicates, and no "read then insert" race exists to begin
 * with (AGENTS.md 3).
 */
@Profile("!executor")
@Component
public class InstanceFactory {

    private static final Logger log = LoggerFactory.getLogger(InstanceFactory.class);

    private final TaskMapper taskMapper;
    private final PayloadTemplateRenderer renderer;
    private final Clock clock;

    public InstanceFactory(TaskMapper taskMapper, PayloadTemplateRenderer renderer, Clock clock) {
        this.taskMapper = taskMapper;
        this.renderer = renderer;
        this.clock = clock;
    }

    /** The identity key of one shard instance of one planned fire time. */
    public static String identityKey(long jobId, LocalDateTime scheduledAt, int shardIndex) {
        return "job:" + jobId + ":" + epochMillis(scheduledAt) + ":" + shardIndex;
    }

    /** The identity key of a manually triggered instance. */
    public static String manualIdentityKey(long jobId, String clientKey) {
        return "manual:" + jobId + ":" + clientKey;
    }

    /**
     * Creates every shard instance of one fire time.
     *
     * <p>Each insert is attempted even when an earlier shard already existed: a partially created fire
     * time (scheduler died half way) is completed on the next attempt instead of being treated as
     * done, which is exactly what an idempotent insert buys.
     *
     * @return the ids of the instances that exist for this fire time, shard order
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<Long> createInstances(JobDefinition job, LocalDateTime scheduledAt) {
        int shardTotal = job.getShardTotal() == null || job.getShardTotal() < 1 ? 1 : job.getShardTotal();
        List<Long> ids = new ArrayList<>(shardTotal);
        LocalDateTime now = LocalDateTime.now(clock);
        int created = 0;
        for (int shard = 0; shard < shardTotal; shard++) {
            String identityKey = identityKey(job.getId(), scheduledAt, shard);
            Task instance = newInstance(job, identityKey, renderer.render(job, shard, shardTotal, scheduledAt),
                    shard, shardTotal, scheduledAt, now);
            taskMapper.insertInstanceIfAbsent(instance);
            // The id is never taken from the insert: it is resolved from the stored row, which is the
            // same row whether this call inserted it or matched it.
            Task stored = taskMapper.findByIdentityKey(identityKey);
            if (stored == null) {
                throw new IllegalStateException(
                        "instance " + identityKey + " was not inserted and cannot be read back");
            }
            if (isFreshInsert(stored, instance)) {
                created++;
            }
            ids.add(stored.getId());
        }
        if (created > 0) {
            log.info("job {} fire time {} created {} instance(s) ({} shards)",
                    job.getId(), scheduledAt, created, shardTotal);
        }
        return ids;
    }

    /**
     * Creates one instance for a manual trigger. The client key makes the trigger idempotent, so a
     * double click does not run the job twice.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ManualInstance createManualInstance(JobDefinition job, String clientKey, String operator) {
        LocalDateTime now = LocalDateTime.now(clock);
        String identityKey = manualIdentityKey(job.getId(), clientKey);
        Task instance = newInstance(job, identityKey, renderer.render(job, 0, 1, now), 0, 1, now, now);
        taskMapper.insertInstanceIfAbsent(instance);
        Task stored = taskMapper.findByIdentityKey(identityKey);
        if (stored == null) {
            throw new IllegalStateException("manual instance " + identityKey + " could not be stored");
        }
        boolean created = isFreshInsert(stored, instance);
        if (created) {
            log.info("job {} manually triggered by {}: instance {} key={}",
                    job.getId(), operator, stored.getId(), clientKey);
        } else {
            log.info("manual trigger of job {} with key {} matched existing instance {}",
                    job.getId(), clientKey, stored.getId());
        }
        return new ManualInstance(stored, created);
    }

    /**
     * Whether {@code stored} is the row this call wrote.
     *
     * <p>The insert itself cannot answer: MySQL reports 0 affected rows for a duplicate, but the
     * JDBC driver counts matched rows (CLIENT_FOUND_ROWS is the Connector/J default) and returns 1
     * for both outcomes, so {@code insertInstanceIfAbsent}'s return value is unusable here. The
     * stored {@code created_at} is the truth: it equals the attempt's {@code now} only when the
     * attempt wrote the row. Two callers inside the same millisecond can both read their own
     * timestamp back - both still converge on one row, which is what the client key promises.
     */
    private static boolean isFreshInsert(Task stored, Task attempt) {
        return stored.getCreatedAt() != null && stored.getCreatedAt().equals(attempt.getCreatedAt());
    }

    /** A manually triggered instance and whether this call created it. */
    public record ManualInstance(Task instance, boolean created) {
    }

    /**
     * The next fire time a definition should have after a claim, and whether the plan itself is
     * finished (a one-off). Returned together so the caller cannot forget that a finished plan has no
     * next fire time and must not stay schedulable.
     */
    public static NextFire nextFire(JobDefinition job, LocalDateTime plannedFireAt) {
        LocalDateTime next = ScheduleCalculator.nextFireAtAfter(job, plannedFireAt);
        if (next == null && job.getScheduleType() == ScheduleType.ONE_TIME) {
            return new NextFire(null, true);
        }
        return new NextFire(next, false);
    }

    /** Outcome of planning the fire time after {@code plannedFireAt}. */
    public record NextFire(LocalDateTime nextFireAt, boolean planFinished) {
    }

    /** Human readable summary for logs and decisions; keeps templates out of log lines. */
    static String describe(JobDefinition job) {
        return "job " + job.getId() + " (" + job.getJobKey() + ") type=" + job.getTaskType()
                + " schedule=" + job.getScheduleType() + " shards=" + job.getShardTotal()
                + " status=" + job.getStatus();
    }

    static boolean isActive(JobDefinition job) {
        return job != null && job.getStatus() == JobStatus.ENABLED;
    }

    static String errorText(Throwable cause) {
        return Messages.describe(cause);
    }

    private static Task newInstance(JobDefinition job, String identityKey, String payload, int shardIndex,
                                    int shardTotal, LocalDateTime scheduledAt, LocalDateTime now) {
        // Timestamps are truncated to milliseconds on the way in: the columns are DATETIME(3), and a
        // system clock with sub-millisecond precision would otherwise be silently cut by MySQL,
        // which breaks the created_at comparison that decides whether a call created its row.
        LocalDateTime millisNow = now.truncatedTo(ChronoUnit.MILLIS);
        Task instance = new Task();
        instance.setTaskType(job.getTaskType());
        instance.setPayload(payload);
        instance.setStatus(TaskStatus.PENDING);
        instance.setPriority(job.getPriority());
        instance.setAttemptCount(0);
        instance.setMaxAttempts(job.getMaxAttempts());
        instance.setNextRunAt(millisNow);
        instance.setTimeoutMs(job.getTimeoutMs());
        instance.setFencingToken(0L);
        instance.setIdentityKey(identityKey);
        instance.setJobId(job.getId());
        instance.setShardIndex(shardIndex);
        instance.setShardTotal(shardTotal);
        instance.setScheduledAt(scheduledAt);
        instance.setExecutorGroup(job.getExecutorGroup());
        instance.setCreatedAt(millisNow);
        instance.setUpdatedAt(millisNow);
        return instance;
    }

    private static long epochMillis(LocalDateTime time) {
        return Duration.between(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC), time).toMillis();
    }
}
