package com.yuki.reactorsched.api.web;

import com.yuki.reactorsched.core.dto.ExecutionLogView;
import com.yuki.reactorsched.core.service.TaskService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RestController;

/**
 * Execution log API (PRD 9, 10.3): the log an executor reported for one attempt.
 *
 * <p>The endpoint is open like the rest of the read API: the log explains an attempt to whoever is
 * looking at it, and it is bounded by the executor before it is ever sent, so a large log is a
 * configured limit rather than an accident.
 */
@Profile("!executor")
@RestController
@RequestMapping("/api/v1/executions")
public class ExecutionLogController {

    private final TaskService taskService;

    public ExecutionLogController(TaskService taskService) {
        this.taskService = taskService;
    }

    @GetMapping("/{id}/log")
    public ExecutionLogView log(@PathVariable long id) {
        return taskService.executionLog(id);
    }
}
