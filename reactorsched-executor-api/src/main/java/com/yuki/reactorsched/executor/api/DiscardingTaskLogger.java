package com.yuki.reactorsched.executor.api;

/**
 * Logger that keeps nothing, for attempts whose log is not collected - the embedded {@code LOCAL}
 * executor of a single-machine deployment (PRD 7.5), or a test that does not assert on logs.
 */
public final class DiscardingTaskLogger implements TaskLogger {

    private static final DiscardingTaskLogger INSTANCE = new DiscardingTaskLogger();

    private DiscardingTaskLogger() {
    }

    public static DiscardingTaskLogger instance() {
        return INSTANCE;
    }

    @Override
    public void log(String line) {
        // Intentionally dropped.
    }

    @Override
    public String text() {
        return "";
    }

    @Override
    public boolean truncated() {
        return false;
    }
}
