package com.yuki.reactorsched.core.service;

import com.yuki.reactorsched.core.decision.SchedulerDecisionRecorder;
import com.yuki.reactorsched.core.domain.DecisionType;
import com.yuki.reactorsched.core.domain.ErrorCodes;
import com.yuki.reactorsched.core.domain.ExecutionResult;
import com.yuki.reactorsched.core.domain.ReasonCodes;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskExecution;
import com.yuki.reactorsched.executor.api.ErrorCategory;
import com.yuki.reactorsched.core.error.TaskFailure;
import com.yuki.reactorsched.core.exception.TaskNotFoundException;
import com.yuki.reactorsched.core.mapper.TaskExecutionMapper;
import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.core.retry.RetryPolicy;
import com.yuki.reactorsched.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Worker facing task operations: claiming work, renewing leases and reporting results.
 *
 * <p>Claiming and result reporting are transactional, and both are guarded by conditional updates
 * carrying the fencing token. A result that belongs to an attempt that no longer owns the task is
 * recorded as {@link ExecutionResult#ABANDONED} and never overwrites task state (PRD 8.1).
 *
 * <p>Failure handling follows PRD 5.4: retryable categories are rescheduled with exponential
 * backoff while attempt budget remains, everything else - and any failure that used the last
 * attempt - moves the task to its terminal {@code FAILED} state.
 */
@Profile("!executor")
@Service
public class TaskOperations {

    private static final Logger log = LoggerFactory.getLogger(TaskOperations.class);

    private final TaskMapper taskMapper;
    private final TaskExecutionMapper executionMapper;
    private final RetryPolicy retryPolicy;
    private final SchedulerDecisionRecorder decisionRecorder;

    public TaskOperations(TaskMapper taskMapper, TaskExecutionMapper executionMapper, RetryPolicy retryPolicy,
                          SchedulerDecisionRecorder decisionRecorder) {
        this.taskMapper = taskMapper;
        this.executionMapper = executionMapper;
        this.retryPolicy = retryPolicy;
        this.decisionRecorder = decisionRecorder;
    }

    /**
     * Candidate tasks of one executor group a worker may attempt to claim, best first.
     *
     * <p>The group is part of the query itself (PRD 7.2): an executor only ever sees candidates of
     * its own group, and the claim repeats the constraint in its {@code WHERE} clause.
     */
    public List<Task> findClaimable(LocalDateTime now, String executorGroup, int limit) {
        if (limit < 1) {
            return List.of();
        }
        return taskMapper.findClaimable(now, executorGroup, limit);
    }

    public Task get(long taskId) {
        Task task = taskMapper.selectById(taskId);
        if (task == null) {
            throw new TaskNotFoundException(taskId);
        }
        return task;
    }

    /**
     * Claims a task of one executor group and opens its execution record in one transaction.
     *
     * <p>The claim is a conditional update carrying the group; when another worker (or an operator
     * cancelling the task) won the race, or the task belongs to another group, this returns
     * {@code null} and nothing is written. The attempt row is created immediately so that a worker
     * dying mid-execution still leaves a trace (PRD 7.2).
     *
     * @param workerId      identity of the claiming executor, stored as the lease owner
     * @param executorGroup group the task must belong to
     */
    @Transactional
    public TaskClaim claimAndOpen(long taskId, String workerId, String executorGroup, LocalDateTime now,
                                  Duration leaseTtl) {
        int claimed = taskMapper.claim(taskId, workerId, executorGroup, now.plus(leaseTtl), now);
        if (claimed != 1) {
            return null;
        }
        Task task = taskMapper.selectById(taskId);
        if (task == null) {
            // Cannot happen inside the same transaction that just updated the row.
            throw new TaskNotFoundException(taskId);
        }
        TaskExecution execution = new TaskExecution();
        execution.setTaskId(taskId);
        execution.setAttempt(task.getAttemptCount());
        execution.setWorkerId(workerId);
        execution.setFencingToken(task.getFencingToken());
        execution.setStartedAt(now);
        executionMapper.insert(execution);
        return new TaskClaim(task, execution);
    }

    /**
     * Reports a successful attempt together with the log the executor produced (PRD 9).
     *
     * @return {@code true} when the task state was written, {@code false} when the result was
     *         rejected because another owner has taken over the task
     */
    @Transactional
    public boolean complete(long taskId, long executionId, long fencingToken, LocalDateTime now,
                            String logText, boolean logTruncated) {
        int updated = taskMapper.markSucceeded(taskId, fencingToken, now);
        if (updated == 1) {
            executionMapper.finish(executionId, ExecutionResult.SUCCEEDED, null, null, null,
                    logText, logTruncated, now);
            return true;
        }
        rejectStale(taskId, executionId, fencingToken, logText, logTruncated, now);
        return false;
    }

    /**
     * Reports a failed attempt and decides what happens next.
     *
     * <p>Retryable failures with attempt budget left go back to {@code RETRY_WAIT} with a backoff
     * delay; non-retryable failures and exhausted attempt budgets end in {@code FAILED} with the
     * {@code MAX_RETRIES_EXCEEDED} code.
     *
     * @return {@code true} when the task state was written, {@code false} when the result was rejected
     */
    @Transactional
    public boolean fail(long taskId, long executionId, long fencingToken, TaskFailure failure,
                        LocalDateTime now, String logText, boolean logTruncated) {
        return fail(taskId, executionId, fencingToken, failure, null, null, now, logText, logTruncated);
    }

    /**
     * Reports a failed attempt, decides what happens next and records the decision (PRD 5.6).
     *
     * <p>The retry decision itself is written to {@code scheduler_decision} while the task state
     * changes, so a reader can explain a delay without guessing it from the task row: a rescheduled
     * attempt becomes {@link DecisionType#RETRY_SCHEDULED} with {@code RETRY_BACKOFF}, a used up
     * retry budget becomes {@link DecisionType#RETRY_EXHAUSTED} with {@code MAX_RETRIES_EXCEEDED}.
     * A failure that is not retryable in the first place is not a scheduling decision, so it only
     * leaves the execution record and {@code task.last_error}.
     *
     * @param workerId         worker that reported the attempt, may be {@code null}
     * @param concurrencyLimit concurrency the worker was dispatching against, may be {@code null}
     * @param logText          executor-reported log of the attempt, may be {@code null}
     * @return {@code true} when the task state was written, {@code false} when the result was rejected
     */
    @Transactional
    public boolean fail(long taskId, long executionId, long fencingToken, TaskFailure failure, String workerId,
                        Integer concurrencyLimit, LocalDateTime now, String logText, boolean logTruncated) {
        Task owned = taskMapper.findOwned(taskId, fencingToken);
        if (owned == null) {
            rejectStale(taskId, executionId, fencingToken, logText, logTruncated, now);
            return false;
        }

        int attempts = owned.getAttemptCount();
        int maxAttempts = owned.getMaxAttempts();
        boolean exhausted = attempts >= maxAttempts;
        String code = failure.code();
        String message = Messages.truncate(failure.message(), Messages.MAX_ERROR_LENGTH);

        if (failure.retryable() && !exhausted) {
            LocalDateTime nextRunAt = retryPolicy.nextRunAt(now, attempts, failure.retryAfterHint());
            String error = Messages.describe(code, failure.category(), message);
            if (taskMapper.markRetryWait(taskId, fencingToken, nextRunAt, error, now) == 1) {
                executionMapper.finish(executionId, ExecutionResult.FAILED, code, failure.category(), message,
                        logText, logTruncated, now);
                log.debug("task {} failed as {} and is retried at {}", taskId, failure.category(), nextRunAt);
                decisionRecorder.recordRetryScheduled(taskId, executionId, workerId, concurrencyLimit, now,
                        nextRunAt, attempts, maxAttempts, failure.category());
                return true;
            }
        } else {
            if (exhausted) {
                code = ErrorCodes.MAX_RETRIES_EXCEEDED;
                message = Messages.truncate(
                        message + " (max attempts reached: " + attempts + "/" + maxAttempts + ")",
                        Messages.MAX_ERROR_LENGTH);
            }
            String error = Messages.describe(code, failure.category(), message);
            if (taskMapper.markFailed(taskId, fencingToken, error, now) == 1) {
                executionMapper.finish(executionId, ExecutionResult.FAILED, code, failure.category(), message,
                        logText, logTruncated, now);
                log.debug("task {} failed terminally as {} ({})", taskId, failure.category(), code);
                if (exhausted && failure.retryable()) {
                    decisionRecorder.recordRetriesExhausted(taskId, executionId, workerId, concurrencyLimit, now,
                            attempts, maxAttempts, failure.category());
                }
                return true;
            }
        }

        rejectStale(taskId, executionId, fencingToken, logText, logTruncated, now);
        return false;
    }

    /**
     * Returns a claimed attempt to the dispatch flow because the executor could not run it: its
     * bounded queue rejected it, or it began shutting down after claiming (PRD 7.4).
     *
     * <p>The instance goes back to {@code RETRY_WAIT} immediately - the executor did not fail at the
     * task, so no backoff is warranted - and the reason is recorded as a {@code TASK_DEFERRAL} with
     * {@code EXECUTOR_REJECTED}, because from the scheduler's point of view the task was deferred by
     * the executor it was meant for.
     *
     * @return {@code true} when the task state was written, {@code false} when the attempt no longer
     *         owns the task
     */
    @Transactional
    public boolean reject(long taskId, long executionId, long fencingToken, String workerId, String cause,
                          LocalDateTime now, String logText, boolean logTruncated) {
        Task owned = taskMapper.findOwned(taskId, fencingToken);
        if (owned == null) {
            rejectStale(taskId, executionId, fencingToken, logText, logTruncated, now);
            return false;
        }
        String error = Messages.describe(ReasonCodes.EXECUTOR_REJECTED, ErrorCategory.EXECUTOR_LOST,
                Messages.truncate(cause, Messages.MAX_ERROR_LENGTH));
        if (taskMapper.markRetryWait(taskId, fencingToken, now, error, now) == 1) {
            executionMapper.finish(executionId, ExecutionResult.FAILED, ReasonCodes.EXECUTOR_REJECTED,
                    ErrorCategory.EXECUTOR_LOST, cause, logText, logTruncated, now);
            decisionRecorder.recordTaskDeferral(taskId, owned.getAttemptCount(), ReasonCodes.EXECUTOR_REJECTED,
                    workerId, null, cause, now);
            return true;
        }
        rejectStale(taskId, executionId, fencingToken, logText, logTruncated, now);
        return false;
    }

    /**
     * Extends the lease of a running attempt (PRD 5.3).
     *
     * @return {@code false} when the attempt no longer owns the task, which tells the worker to stop
     *         working on it
     */
    public boolean renewLease(long taskId, long fencingToken, Duration leaseTtl, LocalDateTime now) {
        return taskMapper.renewLease(taskId, fencingToken, now.plus(leaseTtl), now) == 1;
    }

    private void rejectStale(long taskId, long executionId, long fencingToken, String logText,
                             boolean logTruncated, LocalDateTime now) {
        log.warn("rejected late result for task {}: attempt with fencing token {} no longer owns it",
                taskId, fencingToken);
        executionMapper.finish(executionId,
                ExecutionResult.ABANDONED,
                ErrorCodes.STALE_WRITER,
                ErrorCategory.EXECUTOR_LOST,
                "task is no longer owned by attempt with fencing token " + fencingToken,
                logText, logTruncated, now);
    }
}
