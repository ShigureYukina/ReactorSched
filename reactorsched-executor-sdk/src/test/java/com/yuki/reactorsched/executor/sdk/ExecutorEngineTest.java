package com.yuki.reactorsched.executor.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.executor.api.ExecutionOutcome;
import com.yuki.reactorsched.executor.api.ErrorCategory;
import com.yuki.reactorsched.executor.api.TaskContext;
import com.yuki.reactorsched.executor.api.TaskHandler;
import com.yuki.reactorsched.executor.api.TaskHandlerRegistry;
import com.yuki.reactorsched.executor.api.TransientTaskException;
import com.yuki.reactorsched.executor.sdk.client.ExecutorApiException;
import com.yuki.reactorsched.executor.sdk.client.ExecutorGateway;
import com.yuki.reactorsched.executor.sdk.dto.ClaimTasksRequest;
import com.yuki.reactorsched.executor.sdk.dto.ClaimedTask;
import com.yuki.reactorsched.executor.sdk.dto.ExecutionResultRequest;
import com.yuki.reactorsched.executor.sdk.dto.RegisterExecutorRequest;
import com.yuki.reactorsched.executor.sdk.dto.RegisteredExecutor;
import com.yuki.reactorsched.executor.sdk.dto.RenewLeaseRequest;
import com.yuki.reactorsched.executor.sdk.dto.RenewedLease;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The executor engine against a recording gateway (PRD 7).
 *
 * <p>The engine is started with scheduler intervals of a minute, so the only ticks that run are the
 * ones the test drives: registration, claiming, renewal and shutdown stay deterministic without a
 * live scheduling center.
 */
class ExecutorEngineTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final FakeGateway gateway = new FakeGateway();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private ExecutorEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Test
    void registersWithTheCenterAndUsesTheReturnedId() {
        engine = engineWith(new TestHandler("echo"), properties(2, 10));
        engine.start();

        Awaitility.await().atMost(TIMEOUT).until(() -> engine.getExecutorId() != null);
        assertThat(engine.getExecutorId()).isEqualTo(42L);

        RegisterExecutorRequest request = gateway.registrations.get(0);
        assertThat(gateway.registrations).hasSize(1);
        assertThat(request.appName()).isEqualTo("test-executor");
        assertThat(request.instanceId()).isEqualTo("test-1");
        assertThat(request.group()).isEqualTo("DEFAULT");
        assertThat(request.maxConcurrency()).isEqualTo(2);
        assertThat(request.address()).isNull();
        assertThat(request.metadata()).isNull();
    }

    @Test
    void doesNotClaimBeforeRegistration() {
        engine = engineWith(new TestHandler("echo"), properties(2, 10));

        engine.claimQuietly();

        assertThat(gateway.claimRequests).isEmpty();
    }

    @Test
    void claimsNoMoreThanFreeSlots() throws Exception {
        TestHandler handler = new TestHandler("echo", new CountDownLatch(2), new CountDownLatch(1));
        engine = engineWith(handler, properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo"), task(2L, 12L, 101L, "echo"));
        engine.claimQuietly();
        assertThat(handler.started().await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(gateway.claimRequests).hasSize(1);
        assertThat(gateway.claimRequests.get(0).batchSize()).isEqualTo(2);

        // Both slots are busy: the next round does not even ask the center.
        engine.claimQuietly();
        assertThat(gateway.claimRequests).hasSize(1);

        handler.release().countDown();
        Awaitility.await().atMost(TIMEOUT).until(() -> gateway.results.size() == 2);
        assertThat(gateway.results).allSatisfy(result -> {
            assertThat(result.outcome()).isEqualTo(ExecutionOutcome.SUCCEEDED);
            assertThat(result.errorCode()).isNull();
        });
    }

    @Test
    void returnsTaskWhoseTypeIsAtItsConcurrencyLimit() {
        ExecutorSdkProperties properties = properties(4, 10);
        properties.getTaskTypeLimits().put("echo", 1);
        engine = engineWith(new TestHandler("echo"), properties);
        engine.start();
        awaitRegistered();

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo"), task(2L, 12L, 101L, "echo"));
        engine.claimQuietly();

        Awaitility.await().atMost(TIMEOUT).until(() -> gateway.results.size() == 2);
        ExecutionResultRequest rejected = gateway.results.stream()
                .filter(result -> result.outcome() == ExecutionOutcome.REJECTED)
                .findFirst()
                .orElseThrow();
        assertThat(rejected.message()).contains("echo").contains("concurrency limit");
        assertThat(gateway.results.stream()
                .filter(result -> result.outcome() == ExecutionOutcome.SUCCEEDED)).hasSize(1);
    }

    @Test
    void reportsSuccessWithTheBoundedLog() {
        TestHandler handler = new TestHandler("echo");
        handler.body = context -> context.logger().log("step one");
        engine = engineWith(handler, properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo"));
        engine.claimQuietly();

        Awaitility.await().atMost(TIMEOUT).until(() -> gateway.results.size() == 1);
        ExecutionResultRequest result = gateway.results.get(0);
        assertThat(result.outcome()).isEqualTo(ExecutionOutcome.SUCCEEDED);
        assertThat(result.leaseVersion()).isEqualTo(100L);
        assertThat(result.log()).isEqualTo("step one\n");
        assertThat(result.logTruncated()).isFalse();
        assertThat(result.durationMs()).isGreaterThanOrEqualTo(0L);
        assertThat(result.startedAt()).isNotNull();
        assertThat(result.finishedAt()).isNotNull();
    }

    @Test
    void reportsUnclassifiedFailureAsUnknown() {
        TestHandler handler = new TestHandler("echo");
        handler.body = context -> {
            throw new IllegalStateException("boom");
        };
        engine = engineWith(handler, properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo"));
        engine.claimQuietly();

        Awaitility.await().atMost(TIMEOUT).until(() -> gateway.results.size() == 1);
        ExecutionResultRequest result = gateway.results.get(0);
        assertThat(result.outcome()).isEqualTo(ExecutionOutcome.FAILED);
        assertThat(result.errorCode()).isEqualTo("EXECUTION_ERROR");
        assertThat(result.errorCategory()).isEqualTo(ErrorCategory.UNKNOWN);
        assertThat(result.message()).isEqualTo("boom");
    }

    @Test
    void reportsClassifiedFailureWithItsCategory() {
        TestHandler handler = new TestHandler("echo");
        handler.body = context -> {
            throw new TransientTaskException("downstream reset");
        };
        engine = engineWith(handler, properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo"));
        engine.claimQuietly();

        Awaitility.await().atMost(TIMEOUT).until(() -> gateway.results.size() == 1);
        ExecutionResultRequest result = gateway.results.get(0);
        assertThat(result.outcome()).isEqualTo(ExecutionOutcome.FAILED);
        assertThat(result.errorCategory()).isEqualTo(ErrorCategory.TRANSIENT);
        assertThat(result.message()).isEqualTo("downstream reset");
    }

    @Test
    void reportsTimeoutWhenTheHandlerOverruns() {
        TestHandler handler = new TestHandler("echo");
        handler.body = context -> sleep(2_000);
        engine = engineWith(handler, properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo", 150L));
        engine.claimQuietly();

        Awaitility.await().atMost(TIMEOUT).until(() -> gateway.results.size() == 1);
        ExecutionResultRequest result = gateway.results.get(0);
        assertThat(result.outcome()).isEqualTo(ExecutionOutcome.FAILED);
        assertThat(result.errorCode()).isEqualTo("EXECUTION_TIMEOUT");
        assertThat(result.errorCategory()).isEqualTo(ErrorCategory.TIMEOUT);
        assertThat(result.message()).contains("150");
    }

    @Test
    void stopsWorkingOnAnAttemptWhoseLeaseWasLost() throws Exception {
        TestHandler handler = new TestHandler("echo", new CountDownLatch(1), new CountDownLatch(1));
        engine = engineWith(handler, properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo"));
        engine.claimQuietly();
        assertThat(handler.started().await(5, TimeUnit.SECONDS)).isTrue();

        gateway.renewError = new ExecutorApiException(409, "LEASE_VERSION_STALE", "stale");
        engine.renewQuietly();

        // The handler was interrupted and returned without a result: the owner of the lease decides.
        Awaitility.await().atMost(TIMEOUT).until(() -> engine.getInFlight() == 0);
        assertThat(gateway.results).isEmpty();
        handler.release().countDown();
    }

    @Test
    void renewsTheLeaseOfARunningAttempt() throws Exception {
        TestHandler handler = new TestHandler("echo", new CountDownLatch(1), new CountDownLatch(1));
        engine = engineWith(handler, properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo"));
        engine.claimQuietly();
        assertThat(handler.started().await(5, TimeUnit.SECONDS)).isTrue();

        engine.renewQuietly();

        assertThat(gateway.renewals).hasSize(1);
        assertThat(gateway.renewals.get(0).leaseVersion()).isEqualTo(100L);
        handler.release().countDown();
    }

    @Test
    void dropsAResultTheCenterRejectsAsStale() throws Exception {
        TestHandler handler = new TestHandler("echo");
        engine = engineWith(handler, properties(2, 10));
        engine.start();
        awaitRegistered();
        gateway.resultError = new ExecutorApiException(409, "LEASE_VERSION_STALE", "stale");

        gateway.nextClaims = List.of(task(1L, 11L, 100L, "echo"));
        engine.claimQuietly();

        // The attempt was closed as ABANDONED server-side: the submission is not retried.
        Awaitility.await().atMost(TIMEOUT).until(() -> engine.getInFlight() == 0);
        assertThat(gateway.submitAttempts.get()).isEqualTo(1);
        assertThat(gateway.results).isEmpty();
    }

    @Test
    void reregistersWhenTheHeartbeatFindsNoRow() {
        engine = engineWith(new TestHandler("echo"), properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.heartbeatError = new ExecutorApiException(404, "EXECUTOR_NOT_FOUND", "gone");
        engine.heartbeatQuietly();
        assertThat(engine.getExecutorId()).isNull();

        engine.heartbeatQuietly();
        assertThat(engine.getExecutorId()).isEqualTo(42L);
        assertThat(gateway.registrations).hasSize(2);
    }

    @Test
    void stopsClaimingWhenSweptOffline() {
        engine = engineWith(new TestHandler("echo"), properties(2, 10));
        engine.start();
        awaitRegistered();

        gateway.claimError = new ExecutorApiException(409, "EXECUTOR_OFFLINE", "offline");
        engine.claimQuietly();

        assertThat(engine.getExecutorId()).isNull();
        assertThat(gateway.claimRequests).isEmpty();
    }

    @Test
    void refusesToRunWithoutAToken() {
        ExecutorSdkProperties properties = properties(2, 10);
        properties.setToken("");
        assertThatThrownBy(() -> new ExecutorEngine(gateway, new TaskHandlerRegistry(List.of()),
                properties, objectMapper, Clock.systemUTC()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("token");
    }

    // ------------------------------------------------------------------ helpers

    private void awaitRegistered() {
        Awaitility.await().atMost(TIMEOUT).until(() -> engine.getExecutorId() != null);
    }

    private ExecutorEngine engineWith(TaskHandler handler, ExecutorSdkProperties properties) {
        return new ExecutorEngine(gateway, new TaskHandlerRegistry(List.of(handler)), properties,
                objectMapper, Clock.systemUTC());
    }

    /**
     * Properties with scheduler intervals of a minute: the loops a test does not drive cannot fire
     * while it runs.
     */
    private static ExecutorSdkProperties properties(int maxConcurrency, int batchSize) {
        ExecutorSdkProperties properties = new ExecutorSdkProperties();
        properties.setBaseUrl("http://executor-test:1");
        properties.setToken("executor-test-token");
        properties.setAppName("test-executor");
        properties.setInstanceId("test-1");
        properties.setGroup("DEFAULT");
        properties.setMaxConcurrency(maxConcurrency);
        properties.setBatchSize(batchSize);
        properties.setQueueCapacity(8);
        properties.setHeartbeatInterval(Duration.ofMinutes(1));
        properties.setPollInterval(Duration.ofMinutes(1));
        properties.setLeaseRenewInterval(Duration.ofMinutes(1));
        properties.setShutdownGrace(Duration.ofSeconds(2));
        return properties;
    }

    private static ClaimedTask task(long taskId, long executionId, long fencingToken, String taskType) {
        return task(taskId, executionId, fencingToken, taskType, null);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // The watchdog's interrupt is the point of this case: the handler stops early.
            Thread.currentThread().interrupt();
        }
    }

    private static ClaimedTask task(long taskId, long executionId, long fencingToken, String taskType,
                                    Long timeoutMs) {
        return new ClaimedTask(taskId, executionId, 1, fencingToken, taskType, null, 0, 1, null,
                timeoutMs, null);
    }

    /** Handler whose behavior the test sets per case; the latches keep a case deterministic. */
    private static final class TestHandler implements TaskHandler {

        private final String taskType;
        private final CountDownLatch started;
        private final CountDownLatch release;
        private volatile Consumer<TaskContext> body = context -> {
        };

        private TestHandler(String taskType) {
            this(taskType, null, null);
        }

        private TestHandler(String taskType, CountDownLatch started, CountDownLatch release) {
            this.taskType = taskType;
            this.started = started;
            this.release = release;
        }

        private CountDownLatch started() {
            return started;
        }

        private CountDownLatch release() {
            return release;
        }

        @Override
        public String taskType() {
            return taskType;
        }

        @Override
        public void execute(TaskContext context) throws Exception {
            if (started != null) {
                started.countDown();
            }
            body.accept(context);
            if (release != null) {
                release.await();
            }
        }
    }

    /** Records what the executor would have sent; failures are injected per call. */
    private static final class FakeGateway implements ExecutorGateway {

        private final List<RegisterExecutorRequest> registrations = new CopyOnWriteArrayList<>();
        private final List<ClaimTasksRequest> claimRequests = new CopyOnWriteArrayList<>();
        private final List<ExecutionResultRequest> results = new CopyOnWriteArrayList<>();
        private final List<RenewLeaseRequest> renewals = new CopyOnWriteArrayList<>();
        private final AtomicInteger heartbeats = new AtomicInteger();
        private final AtomicInteger submitAttempts = new AtomicInteger();

        private volatile List<ClaimedTask> nextClaims = List.of();
        private volatile ExecutorApiException heartbeatError;
        private volatile ExecutorApiException claimError;
        private volatile ExecutorApiException resultError;
        private volatile ExecutorApiException renewError;

        @Override
        public RegisteredExecutor register(RegisterExecutorRequest request) {
            registrations.add(request);
            return new RegisteredExecutor(42L, request.appName(), request.instanceId(),
                    request.group(), "ONLINE");
        }

        @Override
        public void heartbeat(long executorId) {
            if (heartbeatError != null) {
                throw heartbeatError;
            }
            heartbeats.incrementAndGet();
        }

        @Override
        public List<ClaimedTask> claim(long executorId, ClaimTasksRequest request) {
            if (claimError != null) {
                throw claimError;
            }
            claimRequests.add(request);
            return nextClaims;
        }

        @Override
        public void submitResult(long executorId, long executionId, ExecutionResultRequest request) {
            submitAttempts.incrementAndGet();
            if (resultError != null) {
                throw resultError;
            }
            results.add(request);
        }

        @Override
        public RenewedLease renewLease(long executorId, long executionId, RenewLeaseRequest request) {
            if (renewError != null) {
                throw renewError;
            }
            renewals.add(request);
            return new RenewedLease(executionId, null);
        }
    }
}
