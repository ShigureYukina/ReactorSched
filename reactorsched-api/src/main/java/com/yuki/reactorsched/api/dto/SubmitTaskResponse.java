package com.yuki.reactorsched.api.dto;

import com.yuki.reactorsched.core.dto.TaskView;

/**
 * Response of {@code POST /api/v1/tasks}. {@code idempotentReplay} distinguishes a newly created
 * task (HTTP 201) from a submission that matched an existing idempotency key (HTTP 200).
 */
public record SubmitTaskResponse(TaskView task, boolean idempotentReplay) {
}
