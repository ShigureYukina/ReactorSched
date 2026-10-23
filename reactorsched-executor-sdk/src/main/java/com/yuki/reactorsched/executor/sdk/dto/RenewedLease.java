package com.yuki.reactorsched.executor.sdk.dto;

import java.time.LocalDateTime;

/**
 * What a lease renewal reads back (PRD 8.2, V1.1 7.2).
 *
 * @param executionId attempt the renewal was for
 * @param leaseUntil  when the renewed lease runs out, so the next renewal can be scheduled before
 *                    the recovery path would take the attempt back
 */
public record RenewedLease(long executionId, LocalDateTime leaseUntil) {
}
