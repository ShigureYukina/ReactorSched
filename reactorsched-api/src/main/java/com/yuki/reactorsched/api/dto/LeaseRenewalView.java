package com.yuki.reactorsched.api.dto;

import java.time.LocalDateTime;

/**
 * Answer of {@code POST /api/v1/executors/{id}/executions/{executionId}/renew} (PRD 8.2, V1.1 7.2).
 *
 * <p>The executor learns when the renewed lease runs out, so it can schedule its next renewal
 * before the recovery path takes the attempt back.
 *
 * @param executionId attempt the renewal was for
 * @param leaseUntil  expiry of the renewed lease
 */
public record LeaseRenewalView(long executionId, LocalDateTime leaseUntil) {
}
