package com.yuki.reactorsched.core.domain;

/**
 * Lifecycle of a job definition (PRD 4.2).
 *
 * <p>Kept separate from {@link TaskStatus} on purpose: a definition state says whether the
 * scheduler may create instances, an instance state says how one attempt is doing. Deriving one
 * from the other is exactly the confusion this project exists to avoid (AGENTS.md 4).
 */
public enum JobStatus {

    /** Participating in scheduling. */
    ENABLED,

    /** Manually stopped; creates no instances and has no next fire time. */
    PAUSED,

    /** Soft deleted. Historical instances stay queryable, the job key stays taken. */
    DELETED
}
