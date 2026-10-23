package com.yuki.reactorsched.executor.registry.dto;

import jakarta.validation.constraints.Positive;

/**
 * Body of {@code POST /api/v1/executors/{id}/tasks/claim} (PRD 7.2).
 *
 * @param batchSize how many instances the executor asks for; the center caps it at the executor's
 *                  free slots, which only the executor itself can know
 */
public record ClaimTasksRequest(@Positive(message = "batchSize must be positive") Integer batchSize) {

    /** Upper bound of one claim request, so a misbehaving executor cannot ask for the whole table. */
    public static final int MAX_BATCH_SIZE = 100;

    public int effectiveBatchSize() {
        if (batchSize == null || batchSize < 1) {
            return 1;
        }
        return Math.min(batchSize, MAX_BATCH_SIZE);
    }
}
