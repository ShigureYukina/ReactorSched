package com.yuki.reactorsched.faultlab;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Shared mutable state of the database fault injection (PRD 8.8). The MyBatis interceptor reads it
 * on every statement, the control endpoint writes it. Volatile fields only: the state is a switch,
 * not a counter.
 */
@Profile("!executor")
@Component
public class DatabaseFaultState {

    private DatabaseFaultMode mode = DatabaseFaultMode.NONE;
    private long latencyMs = 0L;

    public DatabaseFaultMode mode() {
        return mode;
    }

    public long latencyMs() {
        return latencyMs;
    }

    public void set(DatabaseFaultMode mode, Long latencyMs) {
        if (mode != null) {
            this.mode = mode;
        }
        if (latencyMs != null) {
            if (latencyMs < 0) {
                throw new IllegalArgumentException("latencyMs must not be negative");
            }
            this.latencyMs = latencyMs;
        }
    }
}
