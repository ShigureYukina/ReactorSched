package com.yuki.reactorsched.executor.sdk.client;

import com.yuki.reactorsched.executor.sdk.dto.ClaimTasksRequest;
import com.yuki.reactorsched.executor.sdk.dto.ClaimedTask;
import com.yuki.reactorsched.executor.sdk.dto.ExecutionResultRequest;
import com.yuki.reactorsched.executor.sdk.dto.RegisterExecutorRequest;
import com.yuki.reactorsched.executor.sdk.dto.RegisteredExecutor;
import com.yuki.reactorsched.executor.sdk.dto.RenewLeaseRequest;
import com.yuki.reactorsched.executor.sdk.dto.RenewedLease;

import java.util.List;

/**
 * The five calls of the executor contract (PRD 7, 10.3): register, heartbeat, claim, result,
 * renewal.
 *
 * <p>The engine talks to this interface, not to HTTP: a test can hand it a recording fake and
 * observe what the executor would have sent, without a server. The REST implementation lives in
 * {@link RestExecutorGateway}.
 */
public interface ExecutorGateway {

    /** Registers this executor, or takes an existing row back online. */
    RegisteredExecutor register(RegisterExecutorRequest request);

    /** Records one heartbeat; the row must exist. */
    void heartbeat(long executorId);

    /** Asks for up to {@code batchSize} claimable instances of this executor's group. */
    List<ClaimedTask> claim(long executorId, ClaimTasksRequest request);

    /** Reports the outcome of one attempt. */
    void submitResult(long executorId, long executionId, ExecutionResultRequest request);

    /** Extends the lease of a running attempt. */
    RenewedLease renewLease(long executorId, long executionId, RenewLeaseRequest request);
}
