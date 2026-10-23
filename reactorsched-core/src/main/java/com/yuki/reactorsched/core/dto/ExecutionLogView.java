package com.yuki.reactorsched.core.dto;

/**
 * The log of one execution attempt (PRD 9).
 *
 * @param log       the executor-reported log text; empty when the executor produced none
 * @param truncated whether the executor cut the log at its byte bound, so the stored text is only
 *                  the head or the tail of what the handler wrote
 */
public record ExecutionLogView(String log, boolean truncated) {

    public static ExecutionLogView of(String log, boolean truncated) {
        return new ExecutionLogView(log == null ? "" : log, truncated);
    }
}
