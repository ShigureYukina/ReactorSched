package com.yuki.reactorsched.executor.api;

/**
 * What an executor reports back for one attempt (PRD 7.3).
 *
 * <p>The vocabulary is the executor's, not the attempt record's: the scheduling center maps it onto
 * the instance state machine and onto {@code task_attempt.result}.
 */
public enum ExecutionOutcome {

    /** The attempt finished; the center marks the instance SUCCEEDED. */
    SUCCEEDED,

    /** The attempt failed; the center decides between retry backoff and terminal FAILED. */
    FAILED,

    /**
     * The executor refused the attempt after claiming it - its bounded queue rejected it, or it is
     * shutting down. The center returns the instance to RETRY_WAIT and records
     * {@code EXECUTOR_REJECTED} (PRD 7.4).
     */
    REJECTED
}
