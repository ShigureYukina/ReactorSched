package com.yuki.reactorsched.executor.api;

/**
 * Defaults shared by every executor for the execution log (PRD 9).
 */
public final class LogLimits {

    /** Default byte bound of one attempt's log: {@code reactorsched.execution.log-max-bytes}. */
    public static final int DEFAULT_MAX_BYTES = 64 * 1024;

    private LogLimits() {
    }
}
