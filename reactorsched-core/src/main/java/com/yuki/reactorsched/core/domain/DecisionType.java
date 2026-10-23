package com.yuki.reactorsched.core.domain;

/** Kind of scheduling decision recorded in {@code scheduler_decision} (PRD 8.7). */
public enum DecisionType {

    /** The adaptive controller changed how many tasks this process may run at once (PRD 8.5). */
    CONCURRENCY_ADJUSTMENT,

    /**
     * A due instance was left unclaimed this round because a scheduling rule did not admit it yet.
     * The reason code says which rule: {@link ReasonCodes#CAPACITY_LIMIT} or
     * {@link ReasonCodes#PRIORITY_WAIT}.
     */
    TASK_DEFERRAL,

    /** A failed attempt was put back into the dispatch flow after a backoff delay (PRD 7). */
    RETRY_SCHEDULED,

    /** A retryable failure used the last attempt, so the instance reached its terminal state (PRD 7). */
    RETRY_EXHAUSTED,

    /** Recovery took an instance back from an executor that stopped renewing its lease (PRD 8.3). */
    LEASE_RECLAIMED,

    /**
     * The scheduler decided what to do with a fire time it noticed late: skipped it, fired it once or
     * caught up a limited number of missed fire times (PRD 6.4).
     */
    MISFIRE,

    /** A fire time was claimed but no instance could be created from it (PRD 6.2). */
    JOB_TRIGGER,

    /** An instance was not handed to the executor it was meant for, or an executor went offline (PRD 7). */
    EXECUTOR_ASSIGNMENT,

    /** An admission point refused new work because the backlog exceeded its threshold (PRD 8.5). */
    BACKPRESSURE
}
