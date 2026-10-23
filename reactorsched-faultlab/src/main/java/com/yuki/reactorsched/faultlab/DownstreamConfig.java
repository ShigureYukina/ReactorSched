package com.yuki.reactorsched.faultlab;

/**
 * Snapshot of the simulated downstream, returned by the control endpoint and stored in experiment
 * evidence so a run can be reproduced.
 *
 * @param calls    number of calls observed since startup
 * @param failures number of those calls that failed
 */
public record DownstreamConfig(long latencyMs, long jitterMs, long connectHangMs, double errorRate,
                               DownstreamFailureKind failureKind,
                               long retryAfterMs, long seed, long calls, long failures) {
}
