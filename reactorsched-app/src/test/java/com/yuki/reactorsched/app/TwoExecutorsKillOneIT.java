package com.yuki.reactorsched.app;

import com.yuki.reactorsched.app.task.TestHandlersConfiguration;
import com.yuki.reactorsched.app.task.TakeoverTaskHandler;
import com.yuki.reactorsched.core.domain.ErrorCodes;
import com.yuki.reactorsched.core.domain.ExecutionResult;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.core.dto.TaskExecutionView;
import com.yuki.reactorsched.executor.api.TaskHandler;
import com.yuki.reactorsched.executor.api.TaskHandlerRegistry;
import com.yuki.reactorsched.executor.sdk.ExecutorEngine;
import com.yuki.reactorsched.executor.sdk.ExecutorSdkProperties;
import com.yuki.reactorsched.executor.sdk.client.ExecutorGateway;
import com.yuki.reactorsched.executor.sdk.client.RestExecutorGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The S3 headline: two executor processes against one scheduling center, one of them killed, its
 * instances taken over by the survivor (PRD 7, 8.3).
 *
 * <p>The executors are real SDK processes - {@link RestExecutorGateway} over HTTP against the
 * randomly-ported application - not test doubles. The center runs with a short lease and a fast
 * offline sweep so the whole failure chain is observable in seconds:
 *
 * <ol>
 *   <li>executor {@code victim-1} claims the instance and starts executing it (attempt 1 blocks
 *       like a long business call);</li>
 *   <li>the victim is killed: its loops stop at once, so it neither heartbeats nor renews nor
 *       reports - exactly what a crashed process looks like from the center;</li>
 *   <li>the offline sweep takes the victim OFFLINE while the recovery scanner waits for its lease to
 *       expire, then closes the attempt as {@code ABANDONED} + {@code LEASE_EXPIRED} and puts the
 *       instance back to {@code RETRY_WAIT};</li>
 *   <li>{@code survivor-1}, online all along, claims the instance as attempt 2 and finishes it.</li>
 * </ol>
 */
@Import(TestHandlersConfiguration.class)
class TwoExecutorsKillOneIT extends AbstractExecutorIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(45);

    @DynamicPropertySource
    static void fastFailureDetection(DynamicPropertyRegistry registry) {
        // A lease the recovery scanner can expire within the test, and an offline sweep that reacts
        // within a couple of heartbeats.
        registry.add("reactorsched.executor.lease-ttl", () -> "3s");
        registry.add("reactorsched.executor.offline-threshold", () -> "2s");
        registry.add("reactorsched.executor.sweep-interval", () -> "500ms");
        registry.add("reactorsched.executor.heartbeat-interval", () -> "1s");
    }

    @Autowired
    private List<TaskHandler> handlers;

    private ExecutorEngine survivor;
    private ExecutorEngine victim;

    @BeforeEach
    void resetHandlerState() {
        TakeoverTaskHandler.reset();
    }

    @AfterEach
    void stopExecutors() {
        if (survivor != null) {
            survivor.stop();
        }
        if (victim != null) {
            victim.stop();
        }
    }

    @Test
    void survivorTakesOverTheInstancesOfAKilledExecutor() {
        victim = startExecutor("victim-1");
        long taskId = submit(TakeoverTaskHandler.TASK_TYPE, "{}").task().id();

        // 1. The victim claims the instance and is executing it.
        awaitUntil(() -> task(taskId).status() == TaskStatus.RUNNING
                && "victim-1".equals(leaseOwnerOf(taskId)), TIMEOUT);
        await().atMost(TIMEOUT).until(TakeoverTaskHandler::firstAttemptEntered);

        // 2. The survivor is online and idle; the running instance is not claimable by anyone else.
        survivor = startExecutor("survivor-1");
        awaitUntil(() -> survivor.getExecutorId() != null, TIMEOUT);
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.RUNNING);

        // 3. Kill the victim: no more heartbeat, no renewal, no result. From the center's side this
        //    is indistinguishable from the process disappearing.
        victim.stop();
        victim = null;
        // Recovery now watches for the lease the victim can no longer renew.
        startRecoveryScanner();

        // 4. The sweep takes the victim OFFLINE; the lease expires and the instance comes back.
        awaitUntil(() -> "OFFLINE".equals(statusOf("victim-1")), TIMEOUT);
        awaitUntil(() -> task(taskId).status() == TaskStatus.RETRY_WAIT, TIMEOUT);

        List<TaskExecutionView> afterReclaim = executions(taskId);
        assertThat(afterReclaim).hasSize(1);
        assertThat(afterReclaim.get(0).result()).isEqualTo(ExecutionResult.ABANDONED);
        assertThat(afterReclaim.get(0).errorCode()).isEqualTo(ErrorCodes.LEASE_EXPIRED);
        assertThat(afterReclaim.get(0).workerId()).isEqualTo("victim-1");

        // 5. The survivor claims the reclaimed instance as attempt 2 and finishes it. The claim
        //    itself is not awaited directly - it lives for milliseconds - the attempt rows below
        //    are what prove who ran which attempt.
        awaitUntil(() -> task(taskId).status() == TaskStatus.SUCCEEDED, TIMEOUT);
        assertThat(task(taskId).attemptCount()).isEqualTo(2);
        // A finished instance keeps no lease owner; the attempt rows say who ran which attempt.
        assertThat(leaseOwnerOf(taskId)).isNull();

        List<TaskExecutionView> attempts = executions(taskId);
        assertThat(attempts).hasSize(2);
        TaskExecutionView abandoned = attempts.stream()
                .filter(attempt -> attempt.result() == ExecutionResult.ABANDONED)
                .findFirst()
                .orElseThrow();
        assertThat(abandoned.attempt()).isEqualTo(1);
        assertThat(abandoned.workerId()).isEqualTo("victim-1");
        TaskExecutionView takeover = attempts.stream()
                .filter(attempt -> attempt.result() == ExecutionResult.SUCCEEDED)
                .findFirst()
                .orElseThrow();
        assertThat(takeover.attempt()).isEqualTo(2);
        assertThat(takeover.workerId()).isEqualTo("survivor-1");
        assertThat(takeover.logText()).contains("took the instance over");

        // The survivor itself stays online: only the killed process was swept.
        assertThat(statusOf("survivor-1")).isEqualTo("ONLINE");
    }

    // ------------------------------------------------------------------ helpers

    private String statusOf(String instanceId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM executor_registry WHERE instance_id = ?", String.class, instanceId);
    }

    private String leaseOwnerOf(long taskId) {
        return jdbcTemplate.queryForObject(
                "SELECT lease_owner FROM task WHERE id = ?", String.class, taskId);
    }

    /**
     * Starts one SDK executor process against the randomly-ported center. The zero grace period is
     * what makes {@link ExecutorEngine#stop()} a kill rather than a drain: the loops stop
     * immediately and the running attempt is interrupted without a result being reported.
     */
    private ExecutorEngine startExecutor(String instanceId) {
        ExecutorSdkProperties properties = new ExecutorSdkProperties();
        properties.setBaseUrl(baseUrl());
        properties.setToken(EXECUTOR_TOKEN);
        properties.setAppName("kill-one");
        properties.setInstanceId(instanceId);
        properties.setGroup("DEFAULT");
        properties.setMaxConcurrency(1);
        properties.setBatchSize(1);
        properties.setQueueCapacity(4);
        properties.setHeartbeatInterval(Duration.ofMillis(250));
        properties.setPollInterval(Duration.ofMillis(100));
        properties.setLeaseRenewInterval(Duration.ofMillis(500));
        properties.setShutdownGrace(Duration.ZERO);
        properties.setRequestTimeout(Duration.ofSeconds(5));
        ExecutorGateway gateway = new RestExecutorGateway(properties, objectMapper);
        ExecutorEngine engine = new ExecutorEngine(gateway, new TaskHandlerRegistry(handlers),
                properties, objectMapper, clock);
        engine.start();
        return engine;
    }
}
