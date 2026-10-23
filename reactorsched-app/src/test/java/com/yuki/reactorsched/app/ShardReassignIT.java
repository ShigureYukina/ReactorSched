package com.yuki.reactorsched.app;

import com.yuki.reactorsched.app.task.TestHandlersConfiguration;
import com.yuki.reactorsched.app.task.TakeoverTaskHandler;
import com.yuki.reactorsched.core.domain.ErrorCodes;
import com.yuki.reactorsched.core.domain.ExecutionResult;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.core.dto.TaskExecutionView;
import com.yuki.reactorsched.core.mapper.JobDefinitionMapper;
import com.yuki.reactorsched.core.service.InstanceFactory;
import com.yuki.reactorsched.core.service.JobService;
import com.yuki.reactorsched.worker.TaskWorkerEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shard reassignment (PRD 8.4): a sharded instance follows the ordinary lease reclaim path, but the
 * decision names {@code SHARD_REASSIGNED} and the {@code shard_index} never changes - any executor
 * of the group may pick the shard up. One RUNNING shard at a time is already enforced by the
 * conditional claim; the reassignment decision is what makes the handover explainable.
 *
 * <p>The shards are created by the scheduled path (a manual trigger creates a single instance), the
 * same way {@code SchedulerScanIT} drives it: a definition row with {@code next_fire_at = now} and
 * one explicit {@code scanOnce()}.
 */
@Import(TestHandlersConfiguration.class)
class ShardReassignIT extends AbstractIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @Autowired
    private JobService jobService;
    @Autowired
    private JobDefinitionMapper jobDefinitionMapper;
    @Autowired
    private InstanceFactory instanceFactory;

    private TaskWorkerEngine firstWorker;
    private TaskWorkerEngine secondWorker;

    @AfterEach
    void stopWorkers() {
        if (firstWorker != null) {
            firstWorker.stop();
        }
        if (secondWorker != null) {
            secondWorker.stop();
        }
    }

    @Test
    void shardsOfACrashedExecutorAreReassignedWithUnchangedIndexes() {
        long jobId = insertShardedJob();
        var scheduler = new com.yuki.reactorsched.scheduler.JobSchedulerService(jobService,
                jobDefinitionMapper, instanceFactory, schedulerDecisionRecorder,
                new com.yuki.reactorsched.scheduler.SchedulerProperties(),
                java.time.Clock.fixed(LocalDateTime.now(ZoneOffset.UTC).toInstant(ZoneOffset.UTC), ZoneOffset.UTC));
        scheduler.scanOnce();

        List<Long> shardIds = instanceIdsOf(jobId);
        assertThat(shardIds).as("shard_total=2 creates two instances").hasSize(2);

        firstWorker = startWorker(workerProperties(4));
        for (long shardId : shardIds) {
            awaitUntil(() -> task(shardId).status() == TaskStatus.RUNNING, TIMEOUT);
        }
        awaitUntil(TakeoverTaskHandler::firstAttemptEntered, TIMEOUT);

        // The executor dies mid-attempt: nothing is reported, both leases simply stop renewing.
        firstWorker.crash();
        firstWorker = null;
        startRecoveryScanner();

        for (long shardId : shardIds) {
            awaitUntil(() -> task(shardId).status() == TaskStatus.RETRY_WAIT, TIMEOUT);
            List<TaskExecutionView> afterReclaim = executions(shardId);
            assertThat(afterReclaim).hasSize(1);
            assertThat(afterReclaim.get(0).result()).isEqualTo(ExecutionResult.ABANDONED);
            assertThat(afterReclaim.get(0).errorCode()).isEqualTo(ErrorCodes.LEASE_EXPIRED);
        }

        // Every reclaim decision names the reassignment and repeats the unchanged shard index.
        List<Integer> decisionIndexes = jdbcTemplate.queryForList(
                "SELECT shard_index FROM scheduler_decision WHERE reason_code = 'SHARD_REASSIGNED' "
                        + "AND job_id = ? ORDER BY shard_index",
                Integer.class, String.valueOf(jobId));
        assertThat(decisionIndexes).as("one SHARD_REASSIGNED decision per shard").containsExactly(0, 1);

        // Another executor of the group picks both shards up and finishes them, indexes unchanged.
        secondWorker = startWorker(workerProperties(4));
        for (long shardId : shardIds) {
            awaitUntil(() -> task(shardId).status() == TaskStatus.SUCCEEDED, TIMEOUT);
        }
        List<Integer> finalIndexes = jdbcTemplate.queryForList(
                "SELECT shard_index FROM task WHERE id IN (?, ?) ORDER BY shard_index",
                Integer.class, String.valueOf(shardIds.get(0)), String.valueOf(shardIds.get(1)));
        assertThat(finalIndexes).containsExactly(0, 1);
        for (long shardId : shardIds) {
            assertThat(executions(shardId)).hasSize(2);
            assertThat(executions(shardId).get(1).result()).isEqualTo(ExecutionResult.SUCCEEDED);
        }
    }

    /** A two-shard takeover definition due right now; the scan turns it into one instance per shard. */
    private long insertShardedJob() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        jdbcTemplate.update("""
                        INSERT INTO job_definition (job_key, name, task_type, payload_template,
                            schedule_type, misfire_policy, misfire_grace_seconds, shard_total,
                            status, next_fire_at, created_by, updated_by, created_at, updated_at)
                        VALUES ('shard-reassign', 'shard reassign', 'takeover', '{}',
                            'ONE_TIME', 'SKIP', 300, 2,
                            'ENABLED', ?, 'it', 'it', ?, ?)
                        """,
                now, now, now);
        return jdbcTemplate.queryForObject(
                "SELECT id FROM job_definition WHERE job_key = 'shard-reassign'", Long.class);
    }

    private List<Long> instanceIdsOf(long jobId) {
        return jdbcTemplate.queryForList("SELECT id FROM task WHERE job_id = ? ORDER BY shard_index",
                Long.class, String.valueOf(jobId));
    }
}
