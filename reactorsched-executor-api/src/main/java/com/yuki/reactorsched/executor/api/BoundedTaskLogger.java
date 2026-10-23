package com.yuki.reactorsched.executor.api;

import java.nio.charset.StandardCharsets;

/**
 * Bounded in-memory log buffer for one attempt (PRD 9).
 *
 * <p>The buffer keeps at most {@code maxBytes} of text. When a line does not fit, the oldest lines
 * are dropped first, so the tail of an attempt - the part that explains a failure - survives. A
 * single line larger than the whole buffer keeps its head, which is where the message is. Any
 * dropped or cut byte sets {@link #truncated()}.
 *
 * <p>Not thread-safe: one attempt is executed by one thread.
 */
public final class BoundedTaskLogger implements TaskLogger {

    private final int maxBytes;
    private final StringBuilder buffer = new StringBuilder();
    private boolean truncated;

    public BoundedTaskLogger(int maxBytes) {
        if (maxBytes < 1) {
            throw new IllegalArgumentException("maxBytes must be at least 1");
        }
        this.maxBytes = maxBytes;
    }

    @Override
    public void log(String line) {
        if (line == null || line.isEmpty()) {
            return;
        }
        String entry = line + "\n";
        int entryBytes = entry.getBytes(StandardCharsets.UTF_8).length;
        if (entryBytes >= maxBytes) {
            // A single line larger than the whole buffer: keep its head, where the message is.
            dropAll();
            appendCut(entry, maxBytes);
            truncated = true;
            return;
        }
        while (usedBytes() + entryBytes > maxBytes && buffer.length() > 0) {
            dropOldestLine();
            truncated = true;
        }
        buffer.append(entry);
    }

    @Override
    public String text() {
        return buffer.toString();
    }

    @Override
    public boolean truncated() {
        return truncated;
    }

    private int usedBytes() {
        return buffer.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    private void dropOldestLine() {
        int newline = buffer.indexOf("\n");
        if (newline < 0) {
            buffer.setLength(0);
        } else {
            buffer.delete(0, newline + 1);
        }
    }

    private void dropAll() {
        buffer.setLength(0);
    }

    /** Appends at most {@code limit} bytes of the entry, cutting on a character boundary. */
    private void appendCut(String entry, int limit) {
        String candidate = entry;
        while (!candidate.isEmpty() && candidate.getBytes(StandardCharsets.UTF_8).length > limit) {
            candidate = candidate.substring(0, candidate.length() - 1);
        }
        buffer.append(candidate);
    }
}
