package com.yuki.reactorsched.recovery;

import com.yuki.reactorsched.core.decision.SchedulerDecisionRecorder;
import com.yuki.reactorsched.core.domain.ErrorCodes;
import com.yuki.reactorsched.core.domain.ExecutionResult;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskExecution;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.executor.api.ErrorCategory;
import com.yuki.reactorsched.core.mapper.TaskExecutionMapper;
import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.core.statemachine.TaskStateMachine;
import com.yuki.reactorsched.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Applies the state changes of a recovery decision.
 *
 * <p>Every write is a conditional update guarded by the task status, the fencing token and the
 * expired lease, so concurrent scanners (or a worker that finishes at the same moment) can never
 * overwrite a decision somebody else already made. A task whose lease expired is never assumed to
 * be successful: it either goes back into the dispatch flow or reaches {@code FAILED}.
 */
@Profile("!executor")
@Service
public class ExpiredLeaseReclaimer {

    private static final Logger log = LoggerFactory.getLogger(ExpiredLeaseReclaimer.class);

    private final TaskMapper taskMapper;
    private final TaskExecutionMapper executionMapper;
    private final SchedulerDecisionRecorder decisionRecorder;

    public ExpiredLeaseReclaimer(TaskMapper taskMapper, TaskExecutionMapper executionMapper,
                                 SchedulerDecisionRecorder decisionRecorder) {
        this.taskMapper = taskMapper;
        this.executionMapper = executionMapper;
        this.decisionRecorder = decisionRecorder;
    }

    /**
     * Reclaims one task whose lease expired: closes the abandoned attempt and moves the task back
     * into the dispatch flow, unless the attempt budget is used up already.
     */
    @Transactional
    public RecoveryOutcome reclaimExpiredLease(long taskId, long fencingToken, LocalDateTime now) {
        Task task = taskMapper.findOwned(taskId, fencingToken);
        if (task == null) {
            // The owning attempt reported a result (or another scanner was faster) in the meantime.
            return RecoveryOutcome.SKIPPED;
        }
        if (task.getLeaseUntil() == null || task.getLeaseUntil().isAfter(now)) {
            // The worker renewed its lease after the candidate list was read.
            return RecoveryOutcome.SKIPPED;
        }

        int attempts = task.getAttemptCount() == null ? 0 : task.getAttemptCount();
        int maxAttempts = task.getMaxAttempts() == null ? 0 : task.getMaxAttempts();
        boolean exhausted = maxAttempts > 0 && attempts >= maxAttempts;

        TaskStatus target = exhausted ? TaskStatus.FAILED : TaskStatus.RETRY_WAIT;
        TaskStateMachine.requireTransition(TaskStatus.RUNNING, target);

        // Sharded instances follow the same reclaim path, but the decision names the reassignment
        // (PRD 8.4): the shard_index never changes, any executor of the group may pick it up next.
        boolean sharded = task.getShardTotal() != null && task.getShardTotal() > 1;

        String detail = "lease expired at " + task.getLeaseUntil() + " without a renewal from " + task.getLeaseOwner();
        List<TaskExecution> abandoned = closeOpenExecutions(taskId, detail, now);
        Long executionId = abandoned.isEmpty() ? null : abandoned.get(abandoned.size() - 1).getId();

        if (exhausted) {
            String note = Messages.describe(ErrorCodes.LEASE_EXPIRED, ErrorCategory.EXECUTOR_LOST,
                    detail + "; retry limit reached: " + attempts + "/" + maxAttempts);
            if (taskMapper.markLeaseExpiredAsFailed(taskId, fencingToken, note, now) != 1) {
                return RecoveryOutcome.SKIPPED;
            }
            recordReclaim(task, sharded, executionId, now, attempts, maxAttempts, true);
            log.warn("task {} failed after lease expiry: no attempt left ({}/{})", taskId, attempts, maxAttempts);
            return RecoveryOutcome.RECLAIMED_AS_FAILED;
        }

        String note = Messages.describe(ErrorCodes.LEASE_EXPIRED, ErrorCategory.EXECUTOR_LOST, detail);
        if (taskMapper.markLeaseExpiredForRetry(taskId, fencingToken, now, note, now) != 1) {
            return RecoveryOutcome.SKIPPED;
        }
        recordReclaim(task, sharded, executionId, now, attempts, maxAttempts, false);
        log.warn("task {} reclaimed after lease expiry (attempt {}/{}), waiting for the next claim",
                taskId, attempts, maxAttempts);
        return RecoveryOutcome.RECLAIMED_FOR_RETRY;
    }

    /** Emits the decision row: SHARD_REASSIGNED for shards, LEASE_EXPIRED for ordinary instances. */
    private void recordReclaim(Task task, boolean sharded, Long executionId, LocalDateTime now,
                               int attempts, int maxAttempts, boolean exhausted) {
        if (sharded) {
            decisionRecorder.recordShardReassigned(task.getId(), task.getJobId(), task.getShardIndex(),
                    executionId, task.getLeaseOwner(), task.getLeaseUntil(), now, attempts, maxAttempts, exhausted);
        } else {
            decisionRecorder.recordLeaseReclaimed(task.getId(), executionId, task.getLeaseOwner(),
                    task.getLeaseUntil(), now, attempts, maxAttempts, exhausted);
        }
    }

    /**
     * Closes an attempt that was left open even though the task already moved on, so the execution
     * history does not keep a phantom running attempt forever (PRD 5.4, "result write failed").
     */
    @Transactional
    public boolean closeOrphanExecution(long executionId, LocalDateTime now) {
        String message = Messages.describe(ErrorCategory.EXECUTOR_LOST,
                "task state moved on before this attempt reported a result");
        if (executionMapper.finish(executionId, ExecutionResult.ABANDONED, ErrorCodes.ORPHANED_EXECUTION,
                ErrorCategory.EXECUTOR_LOST, message, null, false, now) != 1) {
            return false;
        }
        log.warn("closed orphan execution {} at {}", executionId, now);
        return true;
    }

    private List<TaskExecution> closeOpenExecutions(long taskId, String detail, LocalDateTime now) {
        List<TaskExecution> open = executionMapper.findOpenByTaskId(taskId);
        String message = Messages.describe(ErrorCategory.EXECUTOR_LOST, detail);
        for (TaskExecution execution : open) {
            executionMapper.finish(execution.getId(), ExecutionResult.ABANDONED, ErrorCodes.LEASE_EXPIRED,
                    ErrorCategory.EXECUTOR_LOST, message, null, false, now);
        }
        return open;
    }
}
