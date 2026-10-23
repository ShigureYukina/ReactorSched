package com.yuki.reactorsched.core.decision;

import com.yuki.reactorsched.core.domain.DecisionType;
import com.yuki.reactorsched.core.domain.ReasonCodes;
import com.yuki.reactorsched.core.domain.SchedulerDecision;
import com.yuki.reactorsched.executor.api.ErrorCategory;
import com.yuki.reactorsched.core.mapper.SchedulerDecisionMapper;
import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import com.yuki.reactorsched.core.signals.RuntimeSignals;
import com.yuki.reactorsched.core.signals.SignalSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Writes the scheduling decisions to {@code scheduler_decision} (PRD 8.7).
 *
 * <p>Each row comes from the code that actually took the decision: the retry policy when it
 * reschedules or gives up on a failed attempt, the dispatch loop when it leaves a due instance
 * unclaimed, the recovery scanner when it takes an instance back from an executor that stopped
 * renewing its lease, the scheduler when it decides what to do with a fire time it noticed late. No
 * row is derived from a task status by a later reader.
 *
 * <p>The context of a decision - how much is running, how much is due, what the process observed
 * downstream - is read here, in the same transaction as the state change it explains.
 *
 * <p>A deferral is only recorded once per instance and reason as long as the instance stays on the
 * same attempt: an instance that keeps waiting is explained by one row, not by one row per polling
 * round. That keeps the audit table small without hiding a repeated decision on a later attempt.
 */
@Profile("!executor")
@Service
public class SchedulerDecisionRecorder {

    private static final Logger log = LoggerFactory.getLogger(SchedulerDecisionRecorder.class);

    private static final int MAX_DETAILS_LENGTH = 500;

    /** Upper bound of the in-memory deferral memory; beyond it the guard restarts (worst case: a repeat). */
    private static final int MAX_DEFERRAL_MEMORY = 20_000;

    private final SchedulerDecisionMapper decisionMapper;
    private final TaskMapper taskMapper;
    private final RuntimeSignals signals;

    /**
     * Attached after construction (see {@code ReactorCoreConfiguration}): the recorder is created
     * by component scan with a constructor the tests rely on, and the metrics bean is an
     * auto-configuration that may only exist later. {@code null} means "publish nothing".
     */
    private volatile ReactorMetrics metrics;

    /** {@code taskId|reasonCode} -> attempt count that was already explained, this process only. */
    private final Map<String, Integer> deferralsExplained = new ConcurrentHashMap<>();

    public SchedulerDecisionRecorder(SchedulerDecisionMapper decisionMapper,
                                     TaskMapper taskMapper,
                                     RuntimeSignals signals) {
        this.decisionMapper = decisionMapper;
        this.taskMapper = taskMapper;
        this.signals = signals;
    }

    /** Publishes the decision counter (PRD 8.6 panel "decision reason distribution"); null disables it. */
    public void attachMetrics(ReactorMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * A failed attempt was rescheduled with a backoff delay (PRD 7).
     *
     * @param now       time of the state change, used as the decision time
     * @param nextRunAt earliest time the instance may be claimed again
     */
    public void recordRetryScheduled(long taskId, long executionId, String workerId, Integer concurrencyLimit,
                                     LocalDateTime now, LocalDateTime nextRunAt, int attempt, int maxAttempts,
                                     ErrorCategory category) {
        long delayMillis = Math.max(0L, Duration.between(now, nextRunAt).toMillis());
        String details = "attempt " + attempt + "/" + maxAttempts + " failed as " + category
                + "; backed off by " + delayMillis + " ms, next attempt not before " + nextRunAt;
        write(DecisionType.RETRY_SCHEDULED, ReasonCodes.RETRY_BACKOFF, taskId, null, null, executionId, workerId,
                concurrencyLimit, nextRunAt, details, now);
    }

    /**
     * A retryable failure used the last attempt, so the instance reached {@code FAILED} (PRD 7).
     */
    public void recordRetriesExhausted(long taskId, long executionId, String workerId, Integer concurrencyLimit,
                                       LocalDateTime now, int attempt, int maxAttempts, ErrorCategory category) {
        String details = "attempt " + attempt + "/" + maxAttempts + " failed as " + category
                + "; retry budget exhausted, task failed";
        write(DecisionType.RETRY_EXHAUSTED, ReasonCodes.MAX_RETRIES_EXCEEDED, taskId, null, null, executionId,
                workerId, concurrencyLimit, null, details, now);
    }

    /**
     * Recovery took an instance back from an executor whose lease expired (PRD 8.3).
     *
     * @param executionId  the abandoned attempt that was closed, or {@code null} when none was open
     * @param leaseOwner   executor that stopped renewing the lease
     * @param leaseUntil   the lease that expired
     * @param exhausted    whether the attempt budget was used up, which makes the instance {@code FAILED}
     */
    public void recordLeaseReclaimed(long taskId, Long executionId, String leaseOwner, LocalDateTime leaseUntil,
                                     LocalDateTime now, int attempt, int maxAttempts, boolean exhausted) {
        String details = "lease expired at " + leaseUntil + " without a renewal from " + leaseOwner
                + "; attempt " + attempt + "/" + maxAttempts + (exhausted
                ? ", retry budget exhausted, task failed"
                : ", requeued for the next claim");
        write(DecisionType.LEASE_RECLAIMED, ReasonCodes.LEASE_EXPIRED, taskId, null, null, executionId, leaseOwner,
                null, null, details, now);
    }

    /**
     * A sharded instance was reclaimed after its executor lost the lease (PRD 8.4): the instance
     * re-enters the ordinary dispatch flow with its {@code shard_index} unchanged, so any executor
     * of the group may pick the shard up while the same-fingerprint guarantee holds.
     */
    public void recordShardReassigned(long taskId, Long jobId, Integer shardIndex, Long executionId,
                                      String leaseOwner, LocalDateTime leaseUntil, LocalDateTime now,
                                      int attempt, int maxAttempts, boolean exhausted) {
        String details = "lease expired at " + leaseUntil + " without a renewal from " + leaseOwner
                + "; shard " + shardIndex + " of job " + jobId + " is reassigned with its shard_index unchanged"
                + "; attempt " + attempt + "/" + maxAttempts + (exhausted
                ? ", retry budget exhausted, shard failed"
                : ", requeued for any executor of the group");
        write(DecisionType.LEASE_RECLAIMED, ReasonCodes.SHARD_REASSIGNED, taskId, jobId, shardIndex, executionId,
                leaseOwner, null, null, details, now);
    }

    /**
     * A due instance was not admitted by the dispatch round.
     *
     * @param reasonCode {@link ReasonCodes#CAPACITY_LIMIT} or {@link ReasonCodes#PRIORITY_WAIT}
     * @param cause      which rule deferred it, written into {@code observed_details}
     */
    public void recordTaskDeferral(long taskId, int attempt, String reasonCode, String workerId,
                                   Integer concurrencyLimit, String cause, LocalDateTime now) {
        String key = taskId + "|" + reasonCode;
        Integer explainedAttempt = deferralsExplained.get(key);
        if (explainedAttempt != null && explainedAttempt == attempt) {
            return;
        }
        if (deferralsExplained.size() >= MAX_DEFERRAL_MEMORY) {
            deferralsExplained.clear();
        }
        deferralsExplained.put(key, attempt);

        String details = "attempt " + attempt + " left unclaimed: " + cause;
        write(DecisionType.TASK_DEFERRAL, reasonCode, taskId, null, null, null, workerId, concurrencyLimit, null,
                details, now);
    }

    /**
     * A fire time was noticed too late and the misfire policy decided what happens to it (PRD 6.4).
     *
     * @param reasonCode {@link ReasonCodes#MISFIRE_SKIPPED}, {@link ReasonCodes#MISFIRE_FIRED_LATE} or
     *                   {@link ReasonCodes#MISFIRE_CATCH_UP_LIMIT}
     */
    public void recordMisfire(long jobId, LocalDateTime plannedFireAt, String misfirePolicy, String reasonCode,
                              int shardsPlanned, int missedFireTimes, LocalDateTime now) {
        String details = "fire time " + plannedFireAt + " missed by more than the grace period; policy "
                + misfirePolicy + ", " + missedFireTimes + " fire time(s) past their grace period, "
                + shardsPlanned + " shard instance(s) planned";
        write(DecisionType.MISFIRE, reasonCode, null, jobId, null, null, null, null, null, details, now);
    }

    /**
     * An admission point (instance submission or manual trigger) refused work because the backlog
     * exceeded its threshold (PRD 8.5). The decision is instance-less: it explains why the API
     * answered 429.
     */
    public void recordBackpressureRejected(String entryPoint, long dueBacklog, long waitingQueue,
                                           long backlogThreshold, long queueThreshold, LocalDateTime now) {
        String details = entryPoint + " rejected work: due backlog " + dueBacklog
                + " (reject threshold " + backlogThreshold + "), waiting queue " + waitingQueue
                + " (reject threshold " + queueThreshold + ")";
        write(DecisionType.BACKPRESSURE, ReasonCodes.BACKPRESSURE_REJECTED, null, null, null, null, null, null,
                null, details, now);
    }

    /** A claimed fire time could not be turned into instances; the fire time is lost (PRD 6.2). */
    public void recordJobTriggerFailure(long jobId, LocalDateTime plannedFireAt, String cause,
                                        LocalDateTime now) {        String details = "fire time " + plannedFireAt + " was claimed but instance creation failed: " + cause;
        write(DecisionType.JOB_TRIGGER, ReasonCodes.JOB_TRIGGER_FAILED, null, jobId, null, null, null, null, null,
                details, now);
    }

    /** A shard instance was taken from an executor that went away and will be offered again (PRD 8.4). */
    public void recordShardReassigned(long jobId, long taskId, int shardIndex, String previousExecutor,
                                      LocalDateTime now) {
        String details = "shard " + shardIndex + " of job " + jobId + " was held by " + previousExecutor
                + " when its lease expired; the instance is offered to another executor";
        write(DecisionType.EXECUTOR_ASSIGNMENT, ReasonCodes.SHARD_REASSIGNED, taskId, jobId, shardIndex, null,
                previousExecutor, null, null, details, now);
    }

    /** The executor an instance was meant for stopped sending heartbeats, so nothing was dispatched. */
    public void recordExecutorOffline(long jobId, long taskId, String executorId, LocalDateTime now) {
        String details = "executor " + executorId + " is offline; the instance waits for another executor of the group";
        write(DecisionType.EXECUTOR_ASSIGNMENT, ReasonCodes.EXECUTOR_OFFLINE, taskId, jobId, null, null, executorId,
                null, null, details, now);
    }

    private void write(DecisionType decisionType, String reasonCode, Long taskId, Long jobId, Integer shardIndex,
                       Long executionId, String workerId, Integer concurrencyLimit, LocalDateTime nextRunAt,
                       String details, LocalDateTime now) {
        SchedulerDecision decision = new SchedulerDecision();
        decision.setTaskId(taskId);
        decision.setJobId(jobId);
        decision.setShardIndex(shardIndex);
        decision.setExecutionId(executionId);
        decision.setDecisionType(decisionType);
        decision.setReasonCode(reasonCode);
        decision.setWorkerId(workerId);
        decision.setConcurrencyLimit(concurrencyLimit);
        decision.setNextRunAt(nextRunAt);
        decision.setRunningTasks((int) taskMapper.countRunning());
        decision.setBacklog((int) taskMapper.countDueBacklog(now));
        SignalSnapshot snapshot = signals.snapshot();
        if (snapshot.measured()) {
            decision.setDownstreamLatencyMs(snapshot.averageDownstreamLatencyMillis());
            decision.setDownstreamErrorRate(snapshot.downstreamErrorRate());
        }
        decision.setObservedDetails(truncate(details));
        decision.setCreatedAt(now);
        decisionMapper.insert(decision);
        ReactorMetrics published = this.metrics;
        if (published != null) {
            // After the insert: the counter must not claim a decision the database refused.
            published.decisionRecorded(decisionType.name(), reasonCode);
        }
        log.info("decision {} {} for task {} job {} (attempt record {}): running={} backlog={} concurrencyLimit={} details={}",
                decisionType, reasonCode, taskId, jobId, executionId, decision.getRunningTasks(),
                decision.getBacklog(), concurrencyLimit, decision.getObservedDetails());
    }

    private static String truncate(String details) {
        if (details == null) {
            return null;
        }
        return details.length() <= MAX_DETAILS_LENGTH ? details : details.substring(0, MAX_DETAILS_LENGTH);
    }
}
