package com.yuki.reactorsched.executor.api;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Read-only execution context handed to a {@link TaskHandler}.
 *
 * <p>The payload is the rendered snapshot taken when the instance was created: editing the
 * definition never rewrites what a running attempt sees (AGENTS.md 3).
 *
 * @param taskId     identifier of the logical task
 * @param attempt    attempt number of this execution, starting at 1
 * @param taskType   registered task type
 * @param payload    task parameters as a JSON object; treated as data only
 * @param shardIndex zero-based shard of this instance inside one planned fire time
 * @param shardTotal number of shards the planned fire time was expanded into
 * @param logger     bounded in-memory log of this attempt, reported with the result (PRD 9)
 */
public record TaskContext(long taskId,
                          int attempt,
                          String taskType,
                          JsonNode payload,
                          int shardIndex,
                          int shardTotal,
                          TaskLogger logger) {

    /**
     * Stable business idempotency key of this attempt (PRD 8.1): derived from
     * {@code (taskId, attempt, shardIndex)} so a handler can de-duplicate its downstream calls
     * across retries and reclamations without talking to the scheduling center.
     */
    public String idempotencyKey() {
        return taskId + ":" + attempt + ":" + shardIndex;
    }
}
