package com.yuki.reactorsched.executor.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.executor.api.BoundedTaskLogger;
import com.yuki.reactorsched.executor.api.ExecutionOutcome;
import com.yuki.reactorsched.executor.api.ErrorCategory;
import com.yuki.reactorsched.executor.api.LogLimits;
import com.yuki.reactorsched.executor.api.TaskContext;
import com.yuki.reactorsched.executor.api.TaskFailureException;
import com.yuki.reactorsched.executor.api.TaskHandler;
import com.yuki.reactorsched.executor.api.TaskHandlerRegistry;
import com.yuki.reactorsched.executor.api.UnknownTaskTypeException;
import com.yuki.reactorsched.executor.sdk.client.ExecutorApiException;
import com.yuki.reactorsched.executor.sdk.client.ExecutorGateway;
import com.yuki.reactorsched.executor.sdk.dto.ClaimTasksRequest;
import com.yuki.reactorsched.executor.sdk.dto.ClaimedTask;
import com.yuki.reactorsched.executor.sdk.dto.ExecutionResultRequest;
import com.yuki.reactorsched.executor.sdk.dto.RegisterExecutorRequest;
import com.yuki.reactorsched.executor.sdk.dto.RenewLeaseRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The executor process: registers, heartbeats, pulls work, runs it in a bounded pool, renews
 * leases and reports results (PRD 7).
 *
 * <p><b>Pull, never push.</b> Each round asks for at most as many instances as there are free
 * slots, so the bounded queue stays empty on the normal path and the explicit {@code AbortPolicy}
 * only ever fires when the process is shutting down or a task type cap turned out to be full after
 * the claim - in which case the instance is returned to the center as {@code REJECTED} and goes
 * back to {@code RETRY_WAIT} (PRD 7.4).
 *
 * <p><b>Leases and the watchdog.</b> While an attempt runs, its lease is renewed so no other
 * executor takes the same instance; an attempt that exceeds its timeout is interrupted - a
 * cooperative interrupt, so a handler that ignores it keeps running while the attempt is already
 * reported as timed out, and its late result is rejected by the lease version check (PRD 8.2, 8.3).
 *
 * <p><b>Graceful shutdown</b> stops pulling first, then lets the pool drain for
 * {@code shutdown-grace}, and only then stops renewing: as long as this process keeps its leases,
 * no other executor may start a second execution of the same instance (PRD 7.4). Attempts that do
 * not finish in time are left open on purpose; their leases expire and recovery hands them to
 * another executor.
 */
public class ExecutorEngine implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ExecutorEngine.class);

    /** Code the center answers with when the heartbeat of this executor went stale (PRD 7.1). */
    private static final String CODE_EXECUTOR_OFFLINE = "EXECUTOR_OFFLINE";
    /** Code the center answers with when the lease version no longer owns the instance (PRD 8.2). */
    private static final String CODE_LEASE_VERSION_STALE = "LEASE_VERSION_STALE";
    /** Error code of a failure the handler did not classify itself (PRD 5.4). */
    private static final String CODE_EXECUTION_ERROR = "EXECUTION_ERROR";
    private static final String CODE_EXECUTION_TIMEOUT = "EXECUTION_TIMEOUT";

    private final ExecutorGateway gateway;
    private final TaskHandlerRegistry handlerRegistry;
    private final ExecutorSdkProperties properties;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final String instanceId;

    /** Center-side id of this executor; {@code null} until the first successful registration. */
    private volatile Long executorId;
    private final Map<Long, AttemptHandle> inFlight = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> inFlightByType = new ConcurrentHashMap<>();
    private volatile boolean running;

    private ScheduledExecutorService loopScheduler;
    private ScheduledExecutorService housekeepingScheduler;
    private ThreadPoolExecutor executionPool;

    public ExecutorEngine(ExecutorGateway gateway, TaskHandlerRegistry handlerRegistry,
                          ExecutorSdkProperties properties, ObjectMapper objectMapper, Clock clock) {
        this.gateway = gateway;
        this.handlerRegistry = handlerRegistry;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.instanceId = properties.effectiveInstanceId();
        validate(properties);
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        executionPool = newExecutionPool(properties.getMaxConcurrency(), properties.getQueueCapacity());
        loopScheduler = singleThreadScheduler("reactorsched-executor-loop");
        housekeepingScheduler = singleThreadScheduler("reactorsched-executor-housekeeping");

        long heartbeatMillis = Math.max(1L, properties.getHeartbeatInterval().toMillis());
        long pollMillis = Math.max(1L, properties.getPollInterval().toMillis());
        long renewMillis = Math.max(1L, properties.getLeaseRenewInterval().toMillis());
        loopScheduler.scheduleWithFixedDelay(this::heartbeatQuietly, 0L, heartbeatMillis, TimeUnit.MILLISECONDS);
        loopScheduler.scheduleWithFixedDelay(this::claimQuietly, pollMillis, pollMillis, TimeUnit.MILLISECONDS);
        housekeepingScheduler.scheduleWithFixedDelay(this::renewQuietly, renewMillis, renewMillis,
                TimeUnit.MILLISECONDS);
        running = true;
        log.info("executor {} started: group={} maxConcurrency={} queueCapacity={} batchSize={} "
                        + "heartbeatInterval={} leaseRenewInterval={} shutdownGrace={}",
                instanceId, properties.effectiveGroup(), properties.getMaxConcurrency(),
                properties.getQueueCapacity(), properties.getBatchSize(),
                properties.getHeartbeatInterval(), properties.getLeaseRenewInterval(),
                properties.getShutdownGrace());
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        // 1. Stop pulling: nothing new is claimed from here on.
        if (loopScheduler != null) {
            stopQuietly(loopScheduler);
            loopScheduler = null;
        }
        // 2. Let the pool drain. Queued attempts keep running; renewal keeps their leases alive, so
        //    no other executor may take an instance this process is still working on.
        if (executionPool != null) {
            executionPool.shutdown();
            boolean drained = awaitTermination(executionPool, properties.getShutdownGrace());
            if (!drained) {
                // Grace is over: interrupt whatever still runs. An interrupted handler leaves its
                // attempt open on purpose - the lease expires and recovery hands the instance to
                // another executor (PRD 7.4, 8.3).
                executionPool.shutdownNow();
                log.warn("executor {} stopped with attempts still running; their leases expire and "
                        + "recovery hands the instances to another executor", instanceId);
            }
        }
        // 3. Renewal stops only now, after the last attempt finished (PRD 7.4).
        if (housekeepingScheduler != null) {
            stopQuietly(housekeepingScheduler);
            housekeepingScheduler = null;
        }
        log.info("executor {} stopped", instanceId);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    // ------------------------------------------------------------------ test-visible state

    /** Center-side id once registered, or {@code null} while the executor is registering. */
    public Long getExecutorId() {
        return executorId;
    }

    /** How many attempts this process is executing right now. */
    public int getInFlight() {
        return inFlight.size();
    }

    /** How many attempts of one task type this process is executing right now. */
    public int getInFlight(String taskType) {
        AtomicInteger counter = inFlightByType.get(normalize(taskType));
        return counter == null ? 0 : counter.get();
    }

    /**
     * The bounded execution pool (PRD 7.4). Package-private so a test can assert the queue bound
     * and the rejection policy without going through a live scheduling center.
     */
    ThreadPoolExecutor executionPool() {
        return executionPool;
    }

    /**
     * Builds the bounded pool: fixed size, {@link ArrayBlockingQueue}, explicit {@code AbortPolicy}.
     * An unbounded queue is forbidden - it would let the executor accept work it can never run
     * (PRD 7.4).
     */
    static ThreadPoolExecutor newExecutionPool(int maxConcurrency, int queueCapacity) {
        return new ThreadPoolExecutor(maxConcurrency, maxConcurrency, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                threadFactory("reactorsched-executor-task"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    // ------------------------------------------------------------------ registration and heartbeat

    /** One heartbeat-or-register round. Driven by the scheduler and by deterministic tests. */
    void heartbeatQuietly() {
        try {
            Long id = executorId;
            if (id == null) {
                register();
                return;
            }
            try {
                gateway.heartbeat(id);
            } catch (ExecutorApiException e) {
                if (e.isStatus(404)) {
                    // The row is gone (an operator removed it): re-register instead of heartbeating
                    // into the void.
                    log.warn("executor {} is no longer registered ({}); registering again", instanceId, e.code());
                    executorId = null;
                } else {
                    log.warn("heartbeat of executor {} failed: {} ({})", instanceId, e.getMessage(), e.code());
                }
            }
        } catch (RuntimeException e) {
            log.warn("heartbeat round of executor {} failed: {}", instanceId, e.getMessage());
        }
    }

    private void register() {
        try {
            RegisterExecutorRequest request = new RegisterExecutorRequest(
                    properties.getAppName(), instanceId, properties.effectiveGroup(),
                    properties.getMaxConcurrency(), blankToNull(properties.getAddress()),
                    blankToNull(properties.getMetadata()));
            var registered = gateway.register(request);
            executorId = registered.id();
            log.info("executor {} registered as id {} in group {}", instanceId, registered.id(),
                    registered.groupName());
        } catch (ExecutorApiException e) {
            // The center is unreachable or refuses the registration: try again on the next tick.
            log.warn("registration of executor {} failed: {} ({})", instanceId, e.getMessage(), e.code());
        }
    }

    // ------------------------------------------------------------------ claiming

    /** One claim round. Driven by the scheduler and by deterministic tests. */
    void claimQuietly() {
        try {
            claim();
        } catch (RuntimeException e) {
            log.warn("claim round of executor {} failed: {}", instanceId, e.getMessage());
        }
    }

    private void claim() {
        Long id = executorId;
        if (id == null) {
            return;
        }
        int free = freeSlots();
        if (free <= 0) {
            return;
        }
        int batch = Math.min(free, properties.getBatchSize());
        List<ClaimedTask> tasks;
        try {
            tasks = gateway.claim(id, new ClaimTasksRequest(batch));
        } catch (ExecutorApiException e) {
            if (e.isStatus(404)) {
                executorId = null;
                log.warn("executor {} is no longer registered ({}); registering again", instanceId, e.code());
            } else if (e.isStatus(409) && e.hasCode(CODE_EXECUTOR_OFFLINE)) {
                // Swept OFFLINE while this process is alive (a slow heartbeat, a clock hiccup):
                // re-register on the next heartbeat tick. Attempts still held stay under lease.
                executorId = null;
                log.warn("executor {} was swept OFFLINE; re-registering on the next heartbeat", instanceId);
            } else {
                log.warn("claim of executor {} failed: {} ({})", instanceId, e.getMessage(), e.code());
            }
            return;
        }
        for (ClaimedTask task : tasks) {
            dispatch(id, task);
        }
    }

    /**
     * Slots this process may still fill: concurrency minus what is running and what is queued.
     * Counting the queue is what keeps the bounded queue empty on the normal path - the claim asks
     * for no more than the pool can start right away (PRD 7.4).
     */
    private int freeSlots() {
        ThreadPoolExecutor pool = executionPool;
        if (pool == null) {
            return 0;
        }
        int busy = pool.getActiveCount() + pool.getQueue().size();
        return Math.max(0, properties.getMaxConcurrency() - busy);
    }

    private void dispatch(long executorId, ClaimedTask task) {
        if (typeLimitReached(task.taskType())) {
            // The type cap filled up between the slot computation and the claim: return the
            // instance instead of queueing it behind work that may never run (PRD 7.4).
            log.info("executor {} returns task {}: task type '{}' is at its concurrency limit of {}",
                    instanceId, task.taskId(), task.taskType(), properties.limitFor(task.taskType()));
            report(executorId, task, ExecutionOutcome.REJECTED, null, null,
                    "task type '" + task.taskType() + "' is at its concurrency limit of "
                            + properties.limitFor(task.taskType()), LocalDateTime.now(clock));
            return;
        }
        AttemptHandle handle = new AttemptHandle(task);
        inFlight.put(task.taskId(), handle);
        incrementType(task.taskType());
        try {
            executionPool.execute(() -> runAttempt(executorId, handle));
        } catch (RejectedExecutionException e) {
            // The pool filled up or is shutting down. The claim stays owned by this process until
            // it is returned, so return it now: the center puts it back to RETRY_WAIT.
            log.warn("executor {} could not queue task {}; returning it to the center",
                    instanceId, task.taskId());
            release(handle);
            report(executorId, task, ExecutionOutcome.REJECTED, null, null,
                    "executor queue rejected the attempt", LocalDateTime.now(clock));
        }
    }

    // ------------------------------------------------------------------ execution

    private void runAttempt(long executorId, AttemptHandle handle) {
        handle.bindThread(Thread.currentThread());
        ScheduledFuture<?> watchdog = scheduleWatchdog(handle);
        LocalDateTime startedAt = LocalDateTime.now(clock);
        BoundedTaskLogger logger = new BoundedTaskLogger(LogLimits.DEFAULT_MAX_BYTES);
        try {
            Throwable failure = runHandler(handle, logger);
            LocalDateTime finishedAt = LocalDateTime.now(clock);
            long durationMs = Duration.between(startedAt, finishedAt).toMillis();
            decideOutcome(executorId, handle, failure, logger, startedAt, finishedAt, durationMs);
        } catch (RuntimeException e) {
            // Reporting itself failed; the attempt row stays open on purpose so the inconsistency
            // is visible instead of hidden - recovery closes it later.
            log.error("could not record the outcome of task {} attempt {}",
                    handle.taskId(), handle.task().attempt(), e);
        } finally {
            if (watchdog != null) {
                watchdog.cancel(false);
            }
            handle.markFinished();
            release(handle);
        }
    }

    /** @return the handler failure, or {@code null} when the attempt ran through */
    private Throwable runHandler(AttemptHandle handle, BoundedTaskLogger logger) {
        ClaimedTask task = handle.task();
        try {
            TaskHandler handler = handlerRegistry.require(task.taskType());
            JsonNode payload = task.payload() == null ? objectMapper.createObjectNode() : task.payload();
            handler.execute(new TaskContext(task.taskId(), task.attempt(), task.taskType(), payload,
                    task.shardIndex(), Math.max(1, task.shardTotal()), logger));
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    /**
     * Maps what happened onto one result submission. A timed-out attempt is reported even though
     * its handler may still be running; an attempt whose lease was lost, or that shutdown
     * interrupted, is not reported at all - the owner of the lease decides its outcome (PRD 8.2).
     */
    private void decideOutcome(long executorId, AttemptHandle handle, Throwable failure,
                               BoundedTaskLogger logger, LocalDateTime startedAt, LocalDateTime finishedAt,
                               long durationMs) {
        if (failure == null) {
            if (handle.timedOut()) {
                // The watchdog interrupted a handler that returned successfully afterwards: the
                // attempt was already too slow, so it counts as timed out.
                reportTimeout(executorId, handle, logger, startedAt, finishedAt, durationMs);
            } else if (handle.leaseLost()) {
                log.warn("task {} attempt {} finished after losing its lease; its result is discarded",
                        handle.taskId(), handle.task().attempt());
            } else if (!running) {
                log.info("task {} attempt {} was interrupted by shutdown; the outcome is left to "
                        + "lease recovery", handle.taskId(), handle.task().attempt());
            } else {
                report(executorId, handle.task(), ExecutionOutcome.SUCCEEDED, null, null, null,
                        startedAt, finishedAt, durationMs, logger);
            }
            return;
        }
        if (handle.timedOut()) {
            reportTimeout(executorId, handle, logger, startedAt, finishedAt, durationMs);
            return;
        }
        if (handle.leaseLost()) {
            log.warn("task {} attempt {} failed after losing its lease; its result is discarded",
                    handle.taskId(), handle.task().attempt());
            return;
        }
        if (!running) {
            log.info("task {} attempt {} was interrupted by shutdown; the outcome is left to "
                    + "lease recovery", handle.taskId(), handle.task().attempt());
            return;
        }
        reportFailure(executorId, handle, failure, logger, startedAt, finishedAt, durationMs);
    }

    private void reportTimeout(long executorId, AttemptHandle handle, BoundedTaskLogger logger,
                               LocalDateTime startedAt, LocalDateTime finishedAt, long durationMs) {
        Long timeoutMs = handle.task().timeoutMs();
        log.warn("task {} attempt {} exceeded its timeout of {} ms", handle.taskId(),
                handle.task().attempt(), timeoutMs);
        report(executorId, handle.task(), ExecutionOutcome.FAILED, CODE_EXECUTION_TIMEOUT,
                ErrorCategory.TIMEOUT,
                "execution exceeded the configured timeout of " + (timeoutMs == null ? 0L : timeoutMs) + " ms",
                startedAt, finishedAt, durationMs, logger);
    }

    /**
     * Classifies a handler failure the way the center does (PRD 5.4): a {@link TaskFailureException}
     * carries its own category, everything else is {@link ErrorCategory#UNKNOWN} and therefore
     * retried within the attempt budget.
     */
    private void reportFailure(long executorId, AttemptHandle handle, Throwable failure,
                               BoundedTaskLogger logger, LocalDateTime startedAt, LocalDateTime finishedAt,
                               long durationMs) {
        ErrorCategory category = ErrorCategory.UNKNOWN;
        String code = CODE_EXECUTION_ERROR;
        String message = describe(failure);
        if (failure instanceof TaskFailureException described) {
            category = described.category();
            message = described.getMessage() == null ? category.name() : described.getMessage();
        }
        log.warn("task {} attempt {} failed: {} [category={}]", handle.taskId(),
                handle.task().attempt(), message, category);
        report(executorId, handle.task(), ExecutionOutcome.FAILED, code, category, message,
                startedAt, finishedAt, durationMs, logger);
    }

    private void report(long executorId, ClaimedTask task, ExecutionOutcome outcome, String errorCode,
                        ErrorCategory errorCategory, String message, LocalDateTime startedAt,
                        LocalDateTime finishedAt, long durationMs, BoundedTaskLogger logger) {
        ExecutionResultRequest request = new ExecutionResultRequest(
                task.fencingToken(), outcome, errorCode, errorCategory, message,
                logger == null ? null : logger.text(),
                logger != null && logger.truncated(),
                startedAt, finishedAt, durationMs);
        try {
            gateway.submitResult(executorId, task.executionId(), request);
        } catch (ExecutorApiException e) {
            if (e.isStatus(409) && e.hasCode(CODE_LEASE_VERSION_STALE)) {
                // The lease expired and another executor took over: the attempt was closed as
                // ABANDONED server-side, so there is nothing to retry here (PRD 8.2).
                log.warn("result of task {} attempt {} was rejected as stale; dropping it",
                        task.taskId(), task.attempt());
            } else {
                log.warn("could not report the outcome of task {} attempt {}: {} ({}); the attempt "
                                + "row stays open and lease recovery will handle it",
                        task.taskId(), task.attempt(), e.getMessage(), e.code());
            }
        }
    }

    private void report(long executorId, ClaimedTask task, ExecutionOutcome outcome, String errorCode,
                        ErrorCategory errorCategory, String message, LocalDateTime now) {
        report(executorId, task, outcome, errorCode, errorCategory, message, now, now, 0L, null);
    }

    // ------------------------------------------------------------------ lease renewal and watchdog

    /** One lease-renewal round. Driven by the scheduler and by deterministic tests. */
    void renewQuietly() {
        try {
            renew();
        } catch (RuntimeException e) {
            log.warn("lease renewal round of executor {} failed: {}", instanceId, e.getMessage());
        }
    }

    private void renew() {
        Long id = executorId;
        if (id == null || inFlight.isEmpty()) {
            return;
        }
        for (AttemptHandle handle : inFlight.values()) {
            if (handle.leaseLost() || handle.finished()) {
                continue;
            }
            try {
                gateway.renewLease(id, handle.task().executionId(),
                        new RenewLeaseRequest(handle.task().fencingToken()));
            } catch (ExecutorApiException e) {
                if (e.isStatus(409) && e.hasCode(CODE_LEASE_VERSION_STALE)) {
                    // Another executor owns the instance now: stop working on it. A handler that
                    // ignores the interrupt keeps running, but its late result is rejected by the
                    // lease version check (PRD 8.2).
                    log.warn("executor {} lost the lease of task {}; interrupting the handler",
                            instanceId, handle.taskId());
                    handle.markLeaseLost();
                    handle.interruptThread();
                } else if (e.isStatus(404)) {
                    executorId = null;
                    log.warn("executor {} is no longer registered ({}); registering again",
                            instanceId, e.code());
                    return;
                } else {
                    // A transport hiccup: keep the attempt alive and try again on the next round. If
                    // the outage outlasts the lease, recovery scans the instance like any other.
                    log.warn("could not renew the lease of task {}: {} ({})",
                            handle.taskId(), e.getMessage(), e.code());
                }
            }
        }
    }

    private ScheduledFuture<?> scheduleWatchdog(AttemptHandle handle) {
        Long timeoutMs = handle.task().timeoutMs();
        if (timeoutMs == null || timeoutMs <= 0) {
            return null;
        }
        return housekeepingScheduler.schedule(() -> {
            if (handle.finished() || !handle.markTimedOut()) {
                return;
            }
            log.warn("task {} attempt {} exceeded its timeout of {} ms; interrupting the handler",
                    handle.taskId(), handle.task().attempt(), timeoutMs);
            handle.interruptThread();
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------ bookkeeping

    private void release(AttemptHandle handle) {
        inFlight.remove(handle.taskId(), handle);
        decrementType(handle.task().taskType());
    }

    private boolean typeLimitReached(String taskType) {
        int limit = properties.limitFor(taskType);
        return limit > 0 && getInFlight(taskType) >= limit;
    }

    private void incrementType(String taskType) {
        inFlightByType.computeIfAbsent(normalize(taskType), key -> new AtomicInteger()).incrementAndGet();
    }

    private void decrementType(String taskType) {
        inFlightByType.computeIfAbsent(normalize(taskType), key -> new AtomicInteger()).decrementAndGet();
    }

    private static String normalize(String taskType) {
        return taskType == null ? "" : taskType.trim().toLowerCase(Locale.ROOT);
    }

    private static String describe(Throwable failure) {
        if (failure instanceof UnknownTaskTypeException unknown) {
            return "no handler is registered for task type '" + unknown.getTaskType() + "'";
        }
        String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getName() : message;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static ScheduledExecutorService singleThreadScheduler(String name) {
        return java.util.concurrent.Executors.newSingleThreadScheduledExecutor(threadFactory(name));
    }

    private static void stopQuietly(ScheduledExecutorService scheduler) {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("scheduler {} did not stop within 5s", scheduler);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean awaitTermination(ThreadPoolExecutor pool, Duration grace) {
        try {
            return pool.awaitTermination(Math.max(1L, grace.toMillis()), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
            return false;
        }
    }

    private static ThreadFactory threadFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + sequence.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
    }

    private static void validate(ExecutorSdkProperties properties) {
        if (properties.getBaseUrl() == null || properties.getBaseUrl().isBlank()) {
            throw new IllegalArgumentException("reactorsched.executor.base-url must be set");
        }
        if (properties.getToken() == null || properties.getToken().isBlank()) {
            throw new IllegalArgumentException(
                    "reactorsched.executor.token must be set: the center authenticates executors with it");
        }
        if (properties.getMaxConcurrency() < 1) {
            throw new IllegalArgumentException("reactorsched.executor.max-concurrency must be at least 1");
        }
        if (properties.getQueueCapacity() < 1) {
            throw new IllegalArgumentException("reactorsched.executor.queue-capacity must be at least 1");
        }
        if (properties.getBatchSize() < 1) {
            throw new IllegalArgumentException("reactorsched.executor.batch-size must be at least 1");
        }
        requirePositive(properties.getHeartbeatInterval(), "heartbeat-interval");
        requirePositive(properties.getPollInterval(), "poll-interval");
        requirePositive(properties.getLeaseRenewInterval(), "lease-renew-interval");
        // Zero is a meaningful shutdown-grace ("interrupt at once") and a meaningful request timeout
        // ("fail fast"), so these two only reject negatives.
        requireNonNegative(properties.getShutdownGrace(), "shutdown-grace");
        requireNonNegative(properties.getRequestTimeout(), "request-timeout");
        for (Map.Entry<String, Integer> entry : properties.getTaskTypeLimits().entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                throw new IllegalArgumentException("reactorsched.executor.task-type-limits contains a blank task type");
            }
            if (entry.getValue() == null || entry.getValue() < 1) {
                throw new IllegalArgumentException("reactorsched.executor.task-type-limits."
                        + entry.getKey() + " must be at least 1");
            }
        }
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("reactorsched.executor." + name + " must be positive");
        }
    }

    private static void requireNonNegative(Duration value, String name) {
        if (value == null || value.isNegative()) {
            throw new IllegalArgumentException("reactorsched.executor." + name + " must not be negative");
        }
    }

    /** Bookkeeping for one attempt this process is responsible for. */
    private static final class AttemptHandle {

        private final ClaimedTask task;
        private final AtomicBoolean timedOut = new AtomicBoolean();
        private final AtomicBoolean leaseLost = new AtomicBoolean();
        private final AtomicBoolean finished = new AtomicBoolean();
        private volatile Thread thread;

        private AttemptHandle(ClaimedTask task) {
            this.task = task;
        }

        private long taskId() {
            return task.taskId();
        }

        private ClaimedTask task() {
            return task;
        }

        private boolean timedOut() {
            return timedOut.get();
        }

        private boolean markTimedOut() {
            return timedOut.compareAndSet(false, true);
        }

        private boolean leaseLost() {
            return leaseLost.get();
        }

        private void markLeaseLost() {
            leaseLost.set(true);
        }

        private boolean finished() {
            return finished.get();
        }

        private void markFinished() {
            finished.set(true);
        }

        private void bindThread(Thread thread) {
            this.thread = thread;
        }

        private void interruptThread() {
            Thread current = thread;
            if (current != null) {
                current.interrupt();
            }
        }
    }
}
