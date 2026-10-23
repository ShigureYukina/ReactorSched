package com.yuki.reactorsched.app;

import com.yuki.reactorsched.app.task.TestHandlersConfiguration;
import com.yuki.reactorsched.core.domain.ErrorCodes;
import com.yuki.reactorsched.core.domain.ExecutionResult;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.core.dto.TaskExecutionView;
import com.yuki.reactorsched.faultlab.DatabaseFaultMode;
import com.yuki.reactorsched.faultlab.DatabaseFaultState;
import com.yuki.reactorsched.faultlab.DownstreamFailureKind;
import com.yuki.reactorsched.faultlab.MockDownstream;
import com.yuki.reactorsched.worker.TaskWorkerEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;

/**
 * The four fault injections of PRD 8.8 and the recovery paths they prove: a crashed executor leaves
 * its lease to be reclaimed, a duplicate result is refused as {@code STALE_WRITER}, a hanging
 * downstream connect is cut by the cooperative timeout, and a read-only database stalls claiming
 * without killing the worker.
 */
@Import(TestHandlersConfiguration.class)
class FaultInjectionRecoveryIT extends AbstractIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "admin-test";

    @Autowired
    private MockDownstream downstream;

    @Autowired
    private DatabaseFaultState databaseFault;

    private TaskWorkerEngine worker;

    @AfterEach
    void cleanup() {
        if (worker != null) {
            worker.stop();
            worker = null;
        }
        databaseFault.set(DatabaseFaultMode.NONE, 0L);
        downstream.configure(0L, 0.0, DownstreamFailureKind.TRANSIENT, null, null, null, 0L);
    }

    @Test
    void aCrashedExecutorLeavesTheInstanceToLeaseRecovery() {
        // A short lease so the crash is recoverable within the test.
        var properties = workerProperties(2);
        properties.setLeaseTtl(Duration.ofSeconds(2));
        worker = startWorker(properties, retryWith(Duration.ofMillis(100), 0.0));

        // The attempt blocks long enough for the crash to happen mid-execution.
        long taskId = submit("scenario", "{\"sleepMs\": 15000}", 3, 60_000).task().id();
        awaitUntil(() -> task(taskId).status() == TaskStatus.RUNNING, TIMEOUT);

        worker.crash();
        worker = null;
        startRecoveryScanner();

        // Nothing was reported: the attempt stays open until the lease expires, then the instance
        // comes back without a failure verdict from the dead executor.
        awaitUntil(() -> task(taskId).status() == TaskStatus.RETRY_WAIT, TIMEOUT);
        TaskExecutionView attempt = executions(taskId).get(0);
        assertThat(attempt.result()).isEqualTo(ExecutionResult.ABANDONED);
        assertThat(attempt.errorCode()).isEqualTo(ErrorCodes.LEASE_EXPIRED);

        // A new executor finishes the reclaimed instance.
        worker = startWorker(workerProperties(2), retryWith(Duration.ofMillis(100), 0.0));
        awaitUntil(() -> task(taskId).status() == TaskStatus.SUCCEEDED, TIMEOUT);
        assertThat(executions(taskId)).hasSize(2);
    }

    @Test
    void aDuplicateResultIsRefusedAsStaleWriter() throws Exception {
        worker = startWorker(workerProperties(2));
        long taskId = submit("scenario", "{}").task().id();
        awaitStatus(taskId, TaskStatus.SUCCEEDED, TIMEOUT);

        long executionId = executions(taskId).get(0).id();

        String response = mockMvc.perform(MockMvcRequestBuilders
                        .post("/api/v1/fault-lab/executions/" + executionId + "/duplicate-result")
                        .param("operator", "carol").param("reason", "duplicate message injection")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(response).contains("\"rejectedAsStale\":true");
        // The instance is untouched: the duplicate never overwrote the succeeded state.
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    void aHangingDownstreamConnectIsCutByTheCooperativeTimeout() {
        // The connect hangs far beyond the attempt timeout; the watchdog interrupt is what ends it.
        downstream.configure(0L, 0.0, DownstreamFailureKind.TRANSIENT, null, null, null, 60_000L);
        worker = startWorker(workerProperties(2));

        long taskId = submit("downstream", "{}", 2, 500).task().id();
        long startedNanos = System.nanoTime();

        awaitStatus(taskId, TaskStatus.RETRY_WAIT, TIMEOUT);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);

        assertThat(elapsedMillis)
                .as("the cooperative interrupt ends the attempt around its 500ms timeout, not after the hang")
                .isLessThan(30_000L);
        TaskExecutionView attempt = executions(taskId).get(0);
        assertThat(attempt.errorCode()).isEqualTo(ErrorCodes.EXECUTION_TIMEOUT);
    }

    @Test
    void aReadOnlyDatabaseStallsClaimingWithoutKillingTheWorker() throws Exception {
        // Submit before the injection (the submit itself is an INSERT) and start the worker only
        // once the injection is live: the claim's conditional UPDATE fails, the poll round logs and
        // retries, the worker stays alive and the instance stays untouched.
        long taskId = submit("scenario", "{}").task().id();

        mockMvc.perform(MockMvcRequestBuilders.post("/api/v1/fault-lab/database")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{\"operator\":\"dave\",\"reason\":\"read-only injection\","
                                + "\"mode\":\"READ_ONLY\"}"))
                .andExpect(MockMvcResultMatchers.status().isOk());

        worker = startWorker(workerProperties(2));
        Thread.sleep(1_500L);
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.PENDING);
        assertThat(worker.isRunning()).isTrue();

        mockMvc.perform(MockMvcRequestBuilders.post("/api/v1/fault-lab/database")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{\"operator\":\"dave\",\"reason\":\"end read-only injection\","
                                + "\"mode\":\"NONE\"}"))
                .andExpect(MockMvcResultMatchers.status().isOk());

        awaitStatus(taskId, TaskStatus.SUCCEEDED, TIMEOUT);
    }
}
