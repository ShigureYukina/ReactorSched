package com.yuki.reactorsched.core.domain;

/**
 * What happens when the scheduler notices a fire time later than the configured grace period
 * (PRD 6.4).
 */
public enum MisfirePolicy {

    /** Drop the missed fire times and continue with the next one that is still in the future. */
    SKIP,

    /** Create one instance for the most recent missed fire time, regardless of how many were missed. */
    FIRE_ONCE,

    /** Create one instance per missed fire time, up to the configured catch-up limit. */
    CATCH_UP
}
