package com.yuki.reactorsched.executor.registry.service;

import com.yuki.reactorsched.core.domain.ErrorCodes;
import com.yuki.reactorsched.core.domain.TaskExecution;
import com.yuki.reactorsched.executor.api.ErrorCategory;
import com.yuki.reactorsched.core.error.TaskFailure;
import com.yuki.reactorsched.core.exception.ExecutionNotFoundException;
import com.yuki.reactorsched.core.mapper.TaskExecutionMapper;
import com.yuki.reactorsched.core.service.TaskOperations;
import com.yuki.reactorsched.executor.registry.domain.Executor;
import com.yuki.reactorsched.executor.registry.dto.ExecutionResultRequest;
import com.yuki.reactorsched.executor.registry.dto.RenewLeaseRequest;
import com.yuki.reactorsched.executor.registry.exception.ExecutorMismatchException;
import com.yuki.reactorsched.executor.registry.exception.LeaseVersionStaleException;
import com.yuki.reactorsched.executor.registry.properties.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDateTime;

/**
 * The reporting half of the executor contract: results (PRD 7.3, 9) and lease renewal (PRD 8.2).
 *
 * <p>Every write is guarded by the lease version the attempt was claimed with, carried into the
 * {@code WHERE} clause of the conditional update. When the version no longer owns the instance -
 * the lease expired and somebody else took over - the attempt is closed as {@code ABANDONED} with
 * {@code STALE_WRITER} and the caller gets a 409: the executor logs a warning and drops the attempt,
 * it never retries a stale submission.
 *
 * <p>Ownership is checked before anything is written: the attempt's {@code worker_id} must be this
 * executor's {@code instance_id}. The shared executor token authenticates the fleet (PRD 10.3); the
 * attempt owner decides which process of the fleet may write it.
 */
@Profile("!executor")
@Service
public class ExecutorResultService {

    private static final Logger log = LoggerFactory.getLogger(ExecutorResultService.class);

    private final ExecutorRegistryService registryService;
    private final TaskOperations taskOperations;
    private final TaskExecutionMapper executionMapper;
    private final ExecutorProperties properties;
    private final Clock clock;

    public ExecutorResultService(ExecutorRegistryService registryService, TaskOperations taskOperations,
                                 TaskExecutionMapper executionMapper, ExecutorProperties properties, Clock clock) {
        this.registryService = registryService;
        this.taskOperations = taskOperations;
        this.executionMapper = executionMapper;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Reports the outcome of one attempt.
     *
     * <p>Neither the executor's heartbeat status nor its registration age is checked here: an
     * executor that is alive but was just swept OFFLINE must still be able to finish the attempts it
     * holds, and the lease version is the only authority that decides whether its write counts
     * (PRD 8.2).
     *
     * @throws ExecutionNotFoundException   when the attempt id was never written
     * @throws ExecutorMismatchException    when the attempt belongs to another executor
     * @throws LeaseVersionStaleException   when the lease version no longer owns the instance
     */
    public void submitResult(long executorId, long executionId, ExecutionResultRequest request) {
        Executor executor = registryService.requireExisting(executorId);
        TaskExecution execution = requireAttemptOf(executor, executionId);

        LocalDateTime now = LocalDateTime.now(clock);
        LogBound boundedLog = boundLog(request.log(), request.logTruncated());

        long taskId = execution.getTaskId();
        long leaseVersion = request.leaseVersion();
        switch (request.outcome()) {
            case SUCCEEDED -> {
                if (!taskOperations.complete(taskId, executionId, leaseVersion, now, boundedLog.text(), boundedLog.truncated())) {
                    throw new LeaseVersionStaleException(executionId);
                }
            }
            case FAILED -> {
                TaskFailure failure = TaskFailure.of(
                        request.errorCategory() == null ? ErrorCategory.UNKNOWN : request.errorCategory(),
                        defaultCode(request.errorCode()),
                        defaultMessage(request.message()));
                if (!taskOperations.fail(taskId, executionId, leaseVersion, failure, executor.getInstanceId(),
                        null, now, boundedLog.text(), boundedLog.truncated())) {
                    throw new LeaseVersionStaleException(executionId);
                }
            }
            case REJECTED -> {
                String cause = defaultMessage(request.message());
                if (!taskOperations.reject(taskId, executionId, leaseVersion, executor.getInstanceId(), cause,
                        now, boundedLog.text(), boundedLog.truncated())) {
                    throw new LeaseVersionStaleException(executionId);
                }
                log.debug("executor {} rejected attempt {}: {}", executor.getInstanceId(), executionId, cause);
            }
        }
    }

    /**
     * Extends the lease of a running attempt (PRD 5.3, 8.2, V1.1 7.2).
     *
     * @return when the renewed lease runs out, so the executor can schedule its next renewal
     * @throws ExecutionNotFoundException  when the attempt id was never written
     * @throws ExecutorMismatchException   when the attempt belongs to another executor
     * @throws LeaseVersionStaleException  when the lease version no longer owns the instance
     */
    public LocalDateTime renewLease(long executorId, long executionId, RenewLeaseRequest request) {
        Executor executor = registryService.requireExisting(executorId);
        TaskExecution execution = requireAttemptOf(executor, executionId);

        LocalDateTime now = LocalDateTime.now(clock);
        if (!taskOperations.renewLease(execution.getTaskId(), request.leaseVersion(),
                properties.getLeaseTtl(), now)) {
            throw new LeaseVersionStaleException(executionId);
        }
        return now.plus(properties.getLeaseTtl());
    }

    /**
     * Loads the attempt and checks it belongs to this executor.
     *
     * <p>A missing attempt is a client error about the attempt, not about the task; an attempt owned
     * by another instance id is a 403 even though the fleet token is shared.
     */
    private TaskExecution requireAttemptOf(Executor executor, long executionId) {
        TaskExecution execution = executionMapper.selectById(executionId);
        if (execution == null) {
            throw new ExecutionNotFoundException(executionId);
        }
        if (!executor.getInstanceId().equals(execution.getWorkerId())) {
            throw new ExecutorMismatchException(executionId);
        }
        return execution;
    }

    /**
     * Second bound on the reported log. The SDK already bounds what it sends (PRD 9); this is the
     * center's own limit, so a client that talks to the REST endpoint directly cannot write an
     * unbounded blob into the execution history. The cut never splits a UTF-8 sequence.
     */
    private LogBound boundLog(String text, boolean alreadyTruncated) {
        if (text == null || text.isEmpty()) {
            return new LogBound(text, alreadyTruncated);
        }
        int maxBytes = properties.getLogMaxBytes();
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) {
            return new LogBound(text, alreadyTruncated);
        }
        int cut = maxBytes;
        while (cut > 0 && (bytes[cut] & 0xC0) == 0x80) {
            cut--;
        }
        return new LogBound(new String(bytes, 0, cut, StandardCharsets.UTF_8), true);
    }

    private static String defaultCode(String code) {
        return code == null || code.isBlank() ? ErrorCodes.EXECUTION_ERROR : code;
    }

    private static String defaultMessage(String message) {
        return message == null || message.isBlank() ? "reported by executor" : message;
    }

    /** Log text after the center's own bound, with the truncation flag merged. */
    private record LogBound(String text, boolean truncated) {
    }
}
