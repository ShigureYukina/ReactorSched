package com.yuki.reactorsched.executor.sdk.dto;

/**
 * Renewal body of {@code POST /api/v1/executors/{id}/executions/{executionId}/renew} (PRD 8.2, V1.1 7.2).
 *
 * @param leaseVersion lease version the attempt was claimed with; a stale one is answered with 409
 *                     and tells this executor to stop working on the attempt
 */
public record RenewLeaseRequest(long leaseVersion) {
}
