package com.yuki.reactorsched.executor.api;

/**
 * In-memory execution log of one attempt, reported back with the result (PRD 9).
 *
 * <p>The buffer is bounded on purpose: a handler that logs without end must not be able to grow the
 * executor's heap or the attempt row without limit. Implementations report whether the newest lines
 * were dropped, and the stored log carries that flag as {@code log_truncated}.
 *
 * <p>The logger is handed to the handler through {@link TaskContext#logger()}; it is not thread-safe
 * by contract because one attempt is executed by one thread.
 */
public interface TaskLogger {

    /** Appends one line. Longer lines are stored truncated, never rejected. */
    void log(String line);

    /** The buffered log text, oldest line first. */
    String text();

    /** Whether any line was dropped or cut because the buffer was full. */
    boolean truncated();
}
