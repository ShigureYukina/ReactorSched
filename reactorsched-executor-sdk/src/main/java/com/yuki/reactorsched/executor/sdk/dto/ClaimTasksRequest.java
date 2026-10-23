package com.yuki.reactorsched.executor.sdk.dto;

/**
 * Claim body of {@code POST /api/v1/executors/{id}/tasks/claim} (PRD 7.2).
 *
 * @param batchSize how many instances the executor asks for; it never asks for more than its free
 *                  slots, and the center caps the number as well
 */
public record ClaimTasksRequest(int batchSize) {
}
