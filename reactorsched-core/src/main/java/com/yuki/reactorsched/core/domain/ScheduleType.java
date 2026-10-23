package com.yuki.reactorsched.core.domain;

/**
 * How a definition turns into instances (PRD 6.1).
 */
public enum ScheduleType {

    /** Six-field cron expression, seconds included, interpreted in UTC. */
    CRON,

    /** Fixed interval counted from the planned fire time, so delays do not accumulate. */
    FIXED_RATE,

    /** A single planned run; the definition pauses itself afterwards. */
    ONE_TIME
}
