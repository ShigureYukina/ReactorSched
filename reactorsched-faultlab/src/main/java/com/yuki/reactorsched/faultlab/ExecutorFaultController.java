package com.yuki.reactorsched.faultlab;

import com.yuki.reactorsched.core.exception.ExecutionNotFoundException;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskExecution;
import com.yuki.reactorsched.core.mapper.TaskExecutionMapper;
import com.yuki.reactorsched.core.service.OperationAuditService;
import com.yuki.reactorsched.core.service.TaskOperations;
import com.yuki.reactorsched.worker.TaskWorkerEngine;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.LocalDateTime;

/**
 * Executor-level fault injection (PRD 8.8): crash the LOCAL worker of this process, or replay a
 * result with the execution's original fencing token. Both endpoints require ADMIN (SecurityConfig),
 * take an operator, and write an audit row before the fault takes effect.
 */
@Profile("!executor")
@RestController
@RequestMapping("/api/v1/fault-lab")
public class ExecutorFaultController {

    private final ObjectProvider<TaskWorkerEngine> worker;
    private final TaskOperations taskOperations;
    private final TaskExecutionMapper executionMapper;
    private final OperationAuditService auditService;
    private final Clock clock;

    public ExecutorFaultController(ObjectProvider<TaskWorkerEngine> worker, TaskOperations taskOperations,
                                   TaskExecutionMapper executionMapper, OperationAuditService auditService,
                                   Clock clock) {
        this.worker = worker;
        this.taskOperations = taskOperations;
        this.executionMapper = executionMapper;
        this.auditService = auditService;
        this.clock = clock;
    }

    /**
     * Simulates the executor process dying (PRD 8.8): in-flight attempts keep their leases until
     * expiry, then the recovery path reclaims them.
     *
     * <p>{@code workerId} may be omitted in a single-worker process - the common deployment shape -
     * where it resolves to the only LOCAL worker this process runs.
     */
    @PostMapping({"/executor/{workerId}/crash", "/executor/crash"})
    @Transactional
    public CrashOutcome crash(@PathVariable(required = false) String workerId,
                              @RequestParam String operator,
                              @RequestParam String reason) {
        TaskWorkerEngine engine = requireWorker(workerId);
        String resolvedId = engine.getWorkerId();
        // The audit service joins the caller's transaction (MANDATORY), so the endpoint opens one.
        auditService.record(operator, "fault_lab.executor_crash", "worker", resolvedId, reason, null, null);
        engine.crash();
        return new CrashOutcome(resolvedId, "polling and renewal stopped, in-flight attempts stay open "
                + "until their leases expire");
    }

    /**
     * Replays the result of an execution with the fencing token it was opened under (PRD 8.8). If
     * the attempt already reported - or lost its ownership meanwhile - the replay is refused as
     * {@code STALE_WRITER}, which is the evidence the idempotency tests assert on.
     */
    @PostMapping("/executions/{executionId}/duplicate-result")
    @Transactional
    public DuplicateResultOutcome duplicateResult(@PathVariable long executionId,
                                                  @RequestParam String operator,
                                                  @RequestParam String reason) {
        TaskExecution execution = executionMapper.selectById(executionId);
        if (execution == null) {
            throw new ExecutionNotFoundException(executionId);
        }
        Task task = taskOperations.get(execution.getTaskId());
        Long recorded = execution.getFencingToken();
        long replayToken = recorded == null ? task.getFencingToken() : recorded;
        auditService.record(operator, "fault_lab.duplicate_result", "execution", executionId, reason,
                null, recorded);
        boolean applied = taskOperations.complete(execution.getTaskId(), executionId, replayToken,
                LocalDateTime.now(clock), "duplicate result replayed by fault lab (PRD 8.8)", false);
        String note = applied
                ? "the replay was accepted: the attempt still owns the task, so this duplicate completed it"
                : "the replay was refused as STALE_WRITER: the attempt no longer owns the task";
        return new DuplicateResultOutcome(executionId, execution.getTaskId(), replayToken, !applied, note);
    }

    private TaskWorkerEngine requireWorker(String workerId) {
        TaskWorkerEngine engine = worker.getIfAvailable();
        if (engine == null) {
            throw new IllegalStateException("no LOCAL worker runs in this process (reactorsched.worker.enabled=false)");
        }
        if (workerId == null || workerId.isBlank()) {
            return engine;
        }
        if (!engine.getWorkerId().equals(workerId)) {
            throw new IllegalArgumentException("unknown worker id " + workerId
                    + "; this process runs " + engine.getWorkerId());
        }
        return engine;
    }

    public record CrashOutcome(String workerId, String note) {
    }

    public record DuplicateResultOutcome(long executionId, long taskId, long replayedFencingToken,
                                         boolean rejectedAsStale, String note) {
    }
}
