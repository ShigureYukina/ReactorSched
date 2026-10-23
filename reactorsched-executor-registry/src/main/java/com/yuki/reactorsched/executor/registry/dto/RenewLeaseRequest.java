package com.yuki.reactorsched.executor.registry.dto;

import jakarta.validation.constraints.Positive;

/**
 * Body of {@code POST /api/v1/executors/{id}/executions/{executionId}/renew} (PRD 8.2, V1.1).
 *
 * @param leaseVersion lease version the attempt was claimed with; a stale one is rejected with 409
 *                     and tells the executor to stop working on the attempt
 */
public record RenewLeaseRequest(@Positive(message = "leaseVersion must be positive") long leaseVersion) {
}
