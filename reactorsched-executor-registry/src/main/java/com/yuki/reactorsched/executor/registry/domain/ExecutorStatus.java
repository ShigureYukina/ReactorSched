package com.yuki.reactorsched.executor.registry.domain;

/**
 * Liveness of a registered executor (PRD 7.1).
 */
public enum ExecutorStatus {

    /** Registered and heartbeating within the offline threshold; eligible for task assignment. */
    ONLINE,

    /** Heartbeat went stale; no new tasks are assigned until it heartbeats again. */
    OFFLINE
}
