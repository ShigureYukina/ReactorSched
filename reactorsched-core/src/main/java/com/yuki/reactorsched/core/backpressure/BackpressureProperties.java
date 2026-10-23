package com.yuki.reactorsched.core.backpressure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Admission thresholds of the entry points (PRD 8.5): when either metric exceeds its threshold,
 * instance submission and manual triggers answer {@code 429} with {@code BACKPRESSURE_REJECTED}
 * instead of accepting work the system cannot absorb. A threshold of {@code 0} disables that
 * dimension; both at zero disables backpressure entirely.
 */
@ConfigurationProperties(prefix = "reactorsched.backpressure")
public class BackpressureProperties {

    /**
     * Reject threshold of the due backlog: instances in {@code PENDING} or {@code RETRY_WAIT}
     * whose {@code next_run_at} has already passed and that no executor has absorbed.
     */
    private long backlogRejectThreshold = 1_000L;

    /** Reject threshold of the waiting queue: instances waiting to run regardless of eligibility. */
    private long queueRejectThreshold = 10_000L;

    public long getBacklogRejectThreshold() {
        return backlogRejectThreshold;
    }

    public void setBacklogRejectThreshold(long backlogRejectThreshold) {
        this.backlogRejectThreshold = backlogRejectThreshold;
    }

    public long getQueueRejectThreshold() {
        return queueRejectThreshold;
    }

    public void setQueueRejectThreshold(long queueRejectThreshold) {
        this.queueRejectThreshold = queueRejectThreshold;
    }
}
