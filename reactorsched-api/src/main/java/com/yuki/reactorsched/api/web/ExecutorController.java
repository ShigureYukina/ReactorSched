package com.yuki.reactorsched.api.web;

import com.yuki.reactorsched.executor.registry.domain.ExecutorStatus;
import com.yuki.reactorsched.executor.registry.dto.ClaimTasksRequest;
import com.yuki.reactorsched.executor.registry.dto.ClaimedTaskView;
import com.yuki.reactorsched.executor.registry.dto.ExecutionResultRequest;
import com.yuki.reactorsched.executor.registry.dto.ExecutorView;
import com.yuki.reactorsched.executor.registry.dto.RegisterExecutorRequest;
import com.yuki.reactorsched.executor.registry.dto.RenewLeaseRequest;
import com.yuki.reactorsched.executor.registry.exception.ExecutorNotFoundException;
import com.yuki.reactorsched.executor.registry.service.ExecutorRegistryService;
import com.yuki.reactorsched.executor.registry.service.ExecutorResultService;
import com.yuki.reactorsched.executor.registry.service.ExecutorTaskService;
import com.yuki.reactorsched.api.dto.LeaseRenewalView;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;

/**
 * The executor-facing half of the REST contract (PRD 7, 10.3).
 *
 * <p>Registration, heartbeat, claim, result and renewal all require the shared executor token
 * ({@code X-ReactorSched-Token}, checked by {@code ExecutorTokenFilter} in the app); the read
 * endpoint is open like the rest of the query API. Executors never touch the database themselves -
 * everything they need travels in these bodies (AGENTS.md 6).
 */
@Profile("!executor")
@RestController
@RequestMapping("/api/v1/executors")
public class ExecutorController {

    private final ExecutorRegistryService registryService;
    private final ExecutorTaskService taskService;
    private final ExecutorResultService resultService;

    public ExecutorController(ExecutorRegistryService registryService, ExecutorTaskService taskService,
                              ExecutorResultService resultService) {
        this.registryService = registryService;
        this.taskService = taskService;
        this.resultService = resultService;
    }

    /**
     * Registers an executor, or takes an existing {@code (app_name, instance_id)} row back online.
     * The stored row is returned so the executor learns the id it will use from now on.
     */
    @PostMapping("/register")
    public ExecutorView register(@Valid @RequestBody RegisterExecutorRequest request) {
        return ExecutorView.from(registryService.register(request));
    }

    /**
     * Records one heartbeat. A heartbeat for an id that no longer exists is a 404: the row can only
     * disappear through operator action, and the executor must re-register rather than keep
     * heartbeating into the void.
     */
    @PostMapping("/{id}/heartbeat")
    public ExecutorView heartbeat(@PathVariable long id) {
        if (!registryService.heartbeat(id)) {
            throw new ExecutorNotFoundException(id);
        }
        return ExecutorView.from(registryService.requireExisting(id));
    }

    /**
     * Hands the executor up to {@code batchSize} claimable instances of its group. An empty list is
     * a normal answer - there was nothing to take - not an error.
     */
    @PostMapping("/{id}/tasks/claim")
    public List<ClaimedTaskView> claim(@PathVariable long id, @Valid @RequestBody ClaimTasksRequest request) {
        return taskService.claimTasks(id, request);
    }

    /**
     * Reports the outcome of one attempt (PRD 7.3, 9). A stale lease version is a 409 and the
     * attempt has already been closed as {@code ABANDONED}: the executor drops it instead of
     * retrying the submission.
     */
    @PostMapping("/{id}/executions/{executionId}/result")
    public ResponseEntity<Void> submitResult(@PathVariable long id,
                                             @PathVariable long executionId,
                                             @Valid @RequestBody ExecutionResultRequest request) {
        resultService.submitResult(id, executionId, request);
        return ResponseEntity.noContent().build();
    }

    /** Extends the lease of a running attempt (PRD 8.2, V1.1 7.2). */
    @PostMapping("/{id}/executions/{executionId}/renew")
    public LeaseRenewalView renewLease(@PathVariable long id,
                                       @PathVariable long executionId,
                                       @Valid @RequestBody RenewLeaseRequest request) {
        LocalDateTime leaseUntil = resultService.renewLease(id, executionId, request);
        return new LeaseRenewalView(executionId, leaseUntil);
    }

    /** Every registered executor, optionally filtered by status; open read API (PRD 10.3). */
    @GetMapping
    public List<ExecutorView> list(@RequestParam(required = false) ExecutorStatus status) {
        return registryService.list(status).stream().map(ExecutorView::from).toList();
    }
}
