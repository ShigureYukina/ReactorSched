package com.yuki.reactorsched.core.retry;

import com.yuki.reactorsched.core.mapper.GroupRateWindowMapper;

import java.time.Clock;

/**
 * Group-level admission control for retried work (PRD 5.4, 12.3): retries are useful, but a burst
 * of retries must not crowd out tasks that have never run. The cap is shared by every executor
 * process of the same group - the count lives in {@code group_rate_window} and admission is a
 * conditional update, so MySQL row contention throttles the group no matter how many processes
 * poll it (PRD 12.3 forbids passing a process-local counter off as global).
 *
 * <p>One window per epoch second; tasks that are denied simply stay in {@code RETRY_WAIT} and are
 * considered again on the next poll. Expired windows are pruned as a side effect so the table
 * stays small without waiting for the retention job (PRD 12.4).
 *
 * <p>Windows align on wall-clock seconds, so clock drift between executor nodes widens the
 * effective window the same way it widens any lease arithmetic; node clock skew is already
 * probed and reported elsewhere (PRD 6.5).
 */
public class GroupRateLimiter {

    /** Rate window rows older than this are pruned on each acquisition. */
    private static final long WINDOW_TTL_SECONDS = 120L;

    private final GroupRateWindowMapper mapper;
    private final String executorGroup;
    private final int maxPerSecond;
    private final Clock clock;

    public GroupRateLimiter(GroupRateWindowMapper mapper, String executorGroup,
                            RetryProperties properties, Clock clock) {
        this.mapper = mapper;
        this.executorGroup = executorGroup;
        this.maxPerSecond = properties.getMaxDispatchesPerSecond();
        this.clock = clock;
    }

    /**
     * @return {@code true} when a retry of this group may be dispatched in the current window
     */
    public boolean tryAcquire() {
        if (maxPerSecond <= 0) {
            return true;
        }
        long window = clock.instant().getEpochSecond();
        mapper.openWindow(executorGroup, window);
        int admitted = mapper.tryAdmit(executorGroup, window, maxPerSecond);
        mapper.pruneWindows(executorGroup, window - WINDOW_TTL_SECONDS);
        return admitted == 1;
    }

    public int maxPerSecond() {
        return maxPerSecond;
    }

    public String executorGroup() {
        return executorGroup;
    }
}
