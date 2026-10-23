package com.yuki.reactorsched.core.concurrency;

import com.yuki.reactorsched.core.mapper.GroupRateWindowMapper;

import java.time.LocalDateTime;

/**
 * Reads and writes the AIMD concurrency limit that an executor group shares (PRD 8.5, 12.3). The
 * value lives in {@code group_rate_window}'s permanent state row: the adaptive controller writes it
 * after every adjustment, and every executor of the group re-reads it on each poll round, so a
 * limit change reaches every process instead of staying inside the one that computed it.
 */
public class GroupConcurrencyStore {

    /** Sentinel returned by {@link #read} when the group has never published a limit. */
    public static final int NO_LIMIT = -1;

    private final GroupRateWindowMapper mapper;

    public GroupConcurrencyStore(GroupRateWindowMapper mapper) {
        this.mapper = mapper;
    }

    /** Publishes {@code limit} as the group's shared concurrency. */
    public void write(String group, int limit, LocalDateTime now) {
        mapper.upsertGroupLimit(group, limit, now);
    }

    /**
     * @return the group's shared concurrency, or {@link #NO_LIMIT} when nothing has been published
     */
    public int read(String group) {
        Integer limit = mapper.findGroupLimit(group);
        return limit == null ? NO_LIMIT : limit;
    }
}
