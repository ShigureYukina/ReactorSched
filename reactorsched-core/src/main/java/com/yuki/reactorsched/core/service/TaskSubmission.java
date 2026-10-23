package com.yuki.reactorsched.core.service;

import com.yuki.reactorsched.core.dto.TaskView;

/**
 * Result of a submission.
 *
 * @param task    the logical task that exists after the call
 * @param created {@code true} when this call created it, {@code false} when it was an idempotent
 *                replay of an earlier submission
 */
public record TaskSubmission(TaskView task, boolean created) {
}
