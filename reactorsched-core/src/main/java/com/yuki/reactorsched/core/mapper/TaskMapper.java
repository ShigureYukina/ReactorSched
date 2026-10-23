package com.yuki.reactorsched.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskStatus;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Persistence of task state.
 *
 * <p>Every state write is a conditional update: the {@code WHERE} clause carries the state the
 * caller believed it was acting on, so a concurrent worker, an expired lease or a late writer can
 * never overwrite state it no longer owns (PRD 4.3). A return value of {@code 0} means "I lost the
 * race", never "it probably worked".
 */
public interface TaskMapper extends BaseMapper<Task> {

    /**
     * Candidate tasks of one executor group ordered by priority and eligibility time. Both fresh
     * tasks and tasks whose backoff has elapsed are eligible; claiming still decides who wins.
     *
     * <p>The group is part of the query, not a filter applied afterwards: an executor of another
     * group must never even see the candidates, and the claim repeats the constraint so the two
     * cannot drift apart (AGENTS.md 4, PRD 7.2).
     */
    @Select("""
            SELECT *
              FROM task
             WHERE status IN ('PENDING', 'RETRY_WAIT')
               AND next_run_at <= #{now}
               AND executor_group = #{executorGroup}
             ORDER BY priority ASC, next_run_at ASC, id ASC
             LIMIT #{limit}
            """)
    List<Task> findClaimable(@Param("now") LocalDateTime now,
                             @Param("executorGroup") String executorGroup,
                             @Param("limit") int limit);

    /**
     * Idempotent instance creation (PRD 5.3): one statement, one unique key, no read-then-insert race.
     *
     * <p>When {@code identity_key} already exists nothing is written: the {@code ON DUPLICATE KEY
     * UPDATE} clause is a no-op, and the row that now exists is resolved by
     * {@link #findByIdentityKey}. Nothing about the existing instance is touched, so a replay can
     * never disturb the instance it matched.
     *
     * <p>The insert is also attempted for every shard even when an earlier shard already existed: a
     * fire time that was only half created (scheduler died) is completed instead of being treated as
     * done.
     *
     * <p>The return value cannot tell the two outcomes apart: the server reports 0 affected rows for
     * a duplicate, but the JDBC driver counts *matched* rows (CLIENT_FOUND_ROWS is the Connector/J
     * default) and returns 1 either way. Callers must not branch on it - whether this call created
     * the row is answered by comparing the stored row's {@code created_at} with the timestamp of the
     * attempt (see {@code InstanceFactory}).
     *
     * @return the server's affected-row count; 1 for a fresh insert, but also 1 through the driver
     *         when the identity key already existed
     */
    @Insert("""
            INSERT INTO task (id, task_type, payload, status, priority, idempotency_scope, idempotency_key,
                              identity_key, job_id, shard_index, shard_total, scheduled_at, executor_group,
                              attempt_count, max_attempts, next_run_at, timeout_ms, lease_owner, lease_until,
                              fencing_token, last_error, created_at, updated_at)
            VALUES (NULL, #{instance.taskType}, #{instance.payload}, #{instance.status}, #{instance.priority},
                    COALESCE(#{instance.idempotencyScope}, 'default'), #{instance.idempotencyKey},
                    #{instance.identityKey}, #{instance.jobId}, #{instance.shardIndex}, #{instance.shardTotal}, #{instance.scheduledAt}, #{instance.executorGroup},
                    #{instance.attemptCount}, #{instance.maxAttempts}, #{instance.nextRunAt}, #{instance.timeoutMs}, #{instance.leaseOwner}, #{instance.leaseUntil},
                    #{instance.fencingToken}, #{instance.lastError}, #{instance.createdAt}, #{instance.updatedAt})
            ON DUPLICATE KEY UPDATE id = id
            """)
    int insertInstanceIfAbsent(@Param("instance") Task instance);

    @Select("""
            SELECT *
              FROM task
             WHERE identity_key = #{identityKey}
            """)
    Task findByIdentityKey(@Param("identityKey") String identityKey);

    /** Instances created for one planned fire time, shard order. */
    @Select("""
            SELECT *
              FROM task
             WHERE job_id = #{jobId}
               AND scheduled_at = #{scheduledAt}
             ORDER BY shard_index ASC
            """)
    List<Task> findByJobAndSchedule(@Param("jobId") long jobId, @Param("scheduledAt") LocalDateTime scheduledAt);

    /**
     * Atomically takes ownership of an eligible task: the fencing token and attempt counter advance
     * in the same statement that flips the status.
     *
     * <p>The {@code WHERE} clause carries the executor group as well: a task of another group is not
     * claimable by this executor even if it reached this statement, so a routing mistake is stopped
     * by the database instead of by an {@code if} above it (AGENTS.md 6, PRD 7.2).
     *
     * @return 1 when this caller won the task, 0 when it was claimed, cancelled, or belongs to
     *         another group
     */
    @Update("""
            UPDATE task
               SET status = 'RUNNING',
                   lease_owner = #{workerId},
                   lease_until = #{leaseUntil},
                   fencing_token = fencing_token + 1,
                   attempt_count = attempt_count + 1,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status IN ('PENDING', 'RETRY_WAIT')
               AND next_run_at <= #{now}
               AND executor_group = #{executorGroup}
            """)
    int claim(@Param("taskId") long taskId,
              @Param("workerId") String workerId,
              @Param("executorGroup") String executorGroup,
              @Param("leaseUntil") LocalDateTime leaseUntil,
              @Param("now") LocalDateTime now);

    /**
     * Marks the task as succeeded. Requires the task to still be RUNNING under the given fencing
     * token, so an expired attempt cannot confirm a result it no longer owns.
     *
     * @return 1 when the state was written, 0 when it was rejected
     */
    @Update("""
            UPDATE task
               SET status = 'SUCCEEDED',
                   last_error = NULL,
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
            """)
    int markSucceeded(@Param("taskId") long taskId,
                      @Param("fencingToken") long fencingToken,
                      @Param("now") LocalDateTime now);

    /** Marks the task as terminally failed under the given fencing token. */
    @Update("""
            UPDATE task
               SET status = 'FAILED',
                   last_error = #{error},
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
            """)
    int markFailed(@Param("taskId") long taskId,
                   @Param("fencingToken") long fencingToken,
                   @Param("error") String error,
                   @Param("now") LocalDateTime now);

    /**
     * Cancels a task that has not started executing yet. Running tasks are not cancelled here:
     * the state machine has no {@code RUNNING -> CANCELLED} transition (PRD 4.2).
     */
    @Update("""
            UPDATE task
               SET status = 'CANCELLED',
                   last_error = #{note},
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status IN ('PENDING', 'RETRY_WAIT')
            """)
    int markCancelled(@Param("taskId") long taskId,
                      @Param("note") String note,
                      @Param("now") LocalDateTime now);

    /**
     * Administrative replay: a failed task becomes pending again with a fresh attempt budget.
     * The previous error is kept so the task history stays explainable.
     */
    @Update("""
            UPDATE task
               SET status = 'PENDING',
                   attempt_count = 0,
                   next_run_at = #{now},
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'FAILED'
            """)
    int resetForReplay(@Param("taskId") long taskId, @Param("now") LocalDateTime now);

    @Select("""
            SELECT *
              FROM task
             WHERE idempotency_scope = #{scope}
               AND idempotency_key = #{key}
            """)
    Task findByIdempotencyKey(@Param("scope") String scope, @Param("key") String key);

    @Select("""
            <script>
            SELECT COUNT(*)
              FROM task
            <where>
                <if test="status != null">AND status = #{status}</if>
                <if test="from != null">AND created_at &gt;= #{from}</if>
                <if test="to != null">AND created_at &lt; #{to}</if>
            </where>
            </script>
            """)
    long countByQuery(@Param("status") TaskStatus status,
                      @Param("from") LocalDateTime from,
                      @Param("to") LocalDateTime to);

    @Select("""
            <script>
            SELECT *
              FROM task
            <where>
                <if test="status != null">AND status = #{status}</if>
                <if test="from != null">AND created_at &gt;= #{from}</if>
                <if test="to != null">AND created_at &lt; #{to}</if>
            </where>
             ORDER BY created_at DESC, id DESC
             LIMIT #{limit} OFFSET #{offset}
            </script>
            """)
    List<Task> findPage(@Param("status") TaskStatus status,
                        @Param("from") LocalDateTime from,
                        @Param("to") LocalDateTime to,
                        @Param("limit") int limit,
                        @Param("offset") long offset);

    /**
     * Reads a task only if it is still owned by the given attempt. Used to decide what to do with a
     * reported result without acting on a row the caller no longer owns.
     */
    @Select("""
            SELECT *
              FROM task
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
            """)
    Task findOwned(@Param("taskId") long taskId, @Param("fencingToken") long fencingToken);

    /**
     * Sends a task back to {@code RETRY_WAIT} after a retryable failure (PRD 4.2).
     */
    @Update("""
            UPDATE task
               SET status = 'RETRY_WAIT',
                   next_run_at = #{nextRunAt},
                   last_error = #{error},
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
            """)
    int markRetryWait(@Param("taskId") long taskId,
                      @Param("fencingToken") long fencingToken,
                      @Param("nextRunAt") LocalDateTime nextRunAt,
                      @Param("error") String error,
                      @Param("now") LocalDateTime now);

    /**
     * Extends the lease of a running attempt. Only the current owner can renew.
     *
     * @return 1 while the attempt still owns the task, 0 once it has been reclaimed or finished
     */
    @Update("""
            UPDATE task
               SET lease_until = #{leaseUntil},
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
            """)
    int renewLease(@Param("taskId") long taskId,
                   @Param("fencingToken") long fencingToken,
                   @Param("leaseUntil") LocalDateTime leaseUntil,
                   @Param("now") LocalDateTime now);

    /**
     * Running attempts whose lease has run out: candidates for recovery (PRD 5.3).
     */
    @Select("""
            SELECT *
              FROM task
             WHERE status = 'RUNNING'
               AND lease_until IS NOT NULL
               AND lease_until <= #{now}
             ORDER BY lease_until ASC, id ASC
             LIMIT #{limit}
            """)
    List<Task> findExpiredLeases(@Param("now") LocalDateTime now, @Param("limit") int limit);

    /**
     * Reclaims an expired attempt into {@code RETRY_WAIT}. The lease is checked again in the
     * {@code WHERE} clause so the decision only holds if the attempt really is abandoned.
     */
    @Update("""
            UPDATE task
               SET status = 'RETRY_WAIT',
                   next_run_at = #{nextRunAt},
                   last_error = #{error},
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
               AND lease_until <= #{now}
            """)
    int markLeaseExpiredForRetry(@Param("taskId") long taskId,
                                 @Param("fencingToken") long fencingToken,
                                 @Param("nextRunAt") LocalDateTime nextRunAt,
                                 @Param("error") String error,
                                 @Param("now") LocalDateTime now);

    /**
     * Reclaims an expired attempt that has no attempt budget left: it can only end as failed.
     */
    @Update("""
            UPDATE task
               SET status = 'FAILED',
                   last_error = #{error},
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
               AND lease_until <= #{now}
            """)
    int markLeaseExpiredAsFailed(@Param("taskId") long taskId,
                                 @Param("fencingToken") long fencingToken,
                                 @Param("error") String error,
                                 @Param("now") LocalDateTime now);

    /**
     * Tasks that are being executed somewhere right now. Counted across all workers, so it is the
     * real load of the system and not the load of one process.
     */
    @Select("SELECT COUNT(*) FROM task WHERE status = 'RUNNING'")
    long countRunning();

    /**
     * Tasks that are ready to run but have not been claimed yet: the backlog the scheduler is
     * currently not able to absorb (PRD 5.5).
     */
    @Select("""
            SELECT COUNT(*)
              FROM task
             WHERE status IN ('PENDING', 'RETRY_WAIT')
               AND next_run_at <= #{now}
            """)
    long countDueBacklog(@Param("now") LocalDateTime now);

    /** Tasks waiting to run, whether or not their eligibility time has come (the backlog). */
    @Select("""
            SELECT COUNT(*)
              FROM task
             WHERE status IN ('PENDING', 'RETRY_WAIT')
            """)
    long countBacklog();

    @Select("SELECT COUNT(*) FROM task WHERE status = #{status}")
    long countByStatus(@Param("status") TaskStatus status);

    /** Tasks submitted in a window, used for throughput and backlog explanations (PRD 10.2). */
    @Select("""
            SELECT COUNT(*)
              FROM task
             WHERE created_at >= #{from}
               AND created_at < #{to}
            """)
    long countCreatedBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    /**
     * Tasks that reached the given terminal state inside the window. The window is applied to
     * {@code updated_at}, which is the moment the terminal state was written.
     */
    @Select("""
            SELECT COUNT(*)
              FROM task
             WHERE status = #{status}
               AND updated_at >= #{from}
               AND updated_at < #{to}
            """)
    long countReachedStatusBetween(@Param("status") TaskStatus status,
                                   @Param("from") LocalDateTime from,
                                   @Param("to") LocalDateTime to);
}
