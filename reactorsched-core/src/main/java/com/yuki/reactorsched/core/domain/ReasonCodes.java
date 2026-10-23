package com.yuki.reactorsched.core.domain;

/**
 * Reason codes of scheduling decisions (PRD 8.7).
 *
 * <p>A code describes a real decision taken by the scheduling or control code, never something
 * inferred from a task status afterwards. A new code is added to the PRD before it is used here.
 */
public final class ReasonCodes {

    /** Concurrency was reduced because a downstream signal stayed above its threshold. */
    public static final String DOWNSTREAM_OVERLOAD = "DOWNSTREAM_OVERLOAD";

    /** Concurrency was raised again after the observation window passed with healthy signals. */
    public static final String RECOVERY_OBSERVED = "RECOVERY_OBSERVED";

    /** Concurrency fell back to a conservative value because the metrics could not be read. */
    public static final String METRICS_UNAVAILABLE = "METRICS_UNAVAILABLE";

    /** The concurrency quota of this executor (or of the task type) was used up, so the instance waited. */
    public static final String CAPACITY_LIMIT = "CAPACITY_LIMIT";

    /** The batch or priority order of the dispatch round left the instance waiting although quota was free. */
    public static final String PRIORITY_WAIT = "PRIORITY_WAIT";

    /** A failed attempt was rescheduled with a backoff delay and is not ready to run yet (PRD 7). */
    public static final String RETRY_BACKOFF = "RETRY_BACKOFF";

    /** The lease of a running attempt expired and the instance was taken back by recovery. */
    public static final String LEASE_EXPIRED = "LEASE_EXPIRED";

    /** A retryable failure could not be retried because {@code max_attempts} was reached. */
    public static final String MAX_RETRIES_EXCEEDED = "MAX_RETRIES_EXCEEDED";

    /** A fire time was past its grace period and the policy was to drop it (PRD 6.4). */
    public static final String MISFIRE_SKIPPED = "MISFIRE_SKIPPED";

    /** A fire time was past its grace period and the policy was to fire it once, late (PRD 6.4). */
    public static final String MISFIRE_FIRED_LATE = "MISFIRE_FIRED_LATE";

    /** A catch-up stopped at the configured limit; later missed fire times were dropped (PRD 6.4). */
    public static final String MISFIRE_CATCH_UP_LIMIT = "MISFIRE_CATCH_UP_LIMIT";

    /** A shard instance was taken from an executor that went away and handed to another one (PRD 8.4). */
    public static final String SHARD_REASSIGNED = "SHARD_REASSIGNED";

    /** The executor an instance was meant for stopped sending heartbeats, so nothing was dispatched. */
    public static final String EXECUTOR_OFFLINE = "EXECUTOR_OFFLINE";

    /** The executor refused the instance after it was claimed (its thread pool said no). */
    public static final String EXECUTOR_REJECTED = "EXECUTOR_REJECTED";

    /** The admission check refused a submission because the backlog was already too large (PRD 8.5). */
    public static final String BACKPRESSURE_REJECTED = "BACKPRESSURE_REJECTED";

    /** A fire time was claimed but turning it into instances failed; the fire time is lost (PRD 6.2). */
    public static final String JOB_TRIGGER_FAILED = "JOB_TRIGGER_FAILED";

    /** An executor was asked to run an instance of a type it has no handler for (PRD 8.7). */
    public static final String UNKNOWN_TASK_TYPE = "UNKNOWN_TASK_TYPE";

    private ReasonCodes() {
    }
}
