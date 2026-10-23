package com.yuki.reactorsched.executor.registry.mapper;

/**
 * One row of the per-group aggregation behind the executor gauges (PRD 8.6): how many executors of
 * the group are ONLINE and how old its freshest heartbeat is, in seconds.
 */
public record ExecutorGroupStats(String groupName, long onlineCount, Long heartbeatAgeSeconds) {

    /** Age of the group's freshest heartbeat; 0 when it is this very second. */
    public long heartbeatAge() {
        return heartbeatAgeSeconds == null ? 0L : Math.max(0L, heartbeatAgeSeconds);
    }
}
