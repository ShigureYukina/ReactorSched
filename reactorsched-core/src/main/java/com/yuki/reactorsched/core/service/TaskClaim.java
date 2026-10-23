package com.yuki.reactorsched.core.service;

import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskExecution;

/**
 * A claim that succeeded: the task is now RUNNING under {@code task.fencingToken}, and
 * {@code execution} is the attempt row opened for it.
 */
public record TaskClaim(Task task, TaskExecution execution) {
}
