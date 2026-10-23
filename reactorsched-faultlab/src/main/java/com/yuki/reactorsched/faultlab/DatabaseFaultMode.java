package com.yuki.reactorsched.faultlab;

/**
 * Database fault injection modes (PRD 8.8): {@code READ_ONLY} fails every write, {@code LATENCY}
 * slows every statement down, {@code NONE} is the healthy state. Audit writes are always exempt -
 * without them the injection could not be switched off again.
 */
public enum DatabaseFaultMode {
    NONE,
    READ_ONLY,
    LATENCY
}
