package com.yuki.reactorsched.core.statemachine;

import com.yuki.reactorsched.core.domain.JobStatus;

/**
 * Thrown when a caller requests a definition state change the state machine does not allow
 * (PRD 4.2).
 */
public class IllegalJobTransitionException extends RuntimeException {

    private final JobStatus from;
    private final JobStatus to;

    public IllegalJobTransitionException(JobStatus from, JobStatus to) {
        super("illegal job transition: " + from + " -> " + to);
        this.from = from;
        this.to = to;
    }

    public JobStatus getFrom() {
        return from;
    }

    public JobStatus getTo() {
        return to;
    }
}
