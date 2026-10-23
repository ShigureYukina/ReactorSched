package com.yuki.reactorsched.executor.registry.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.domain.TaskExecution;
import com.yuki.reactorsched.core.service.TaskClaim;
import com.yuki.reactorsched.core.service.TaskOperations;
import com.yuki.reactorsched.executor.registry.domain.Executor;
import com.yuki.reactorsched.executor.registry.dto.ClaimTasksRequest;
import com.yuki.reactorsched.executor.registry.dto.ClaimedTaskView;
import com.yuki.reactorsched.executor.registry.properties.ExecutorProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * The claim half of the executor contract (PRD 7.2).
 *
 * <p>An executor asks for work; the center answers with instances of the executor's group and
 * nothing else. The group travels in the query and again in the claim's {@code WHERE} clause, so a
 * misrouted executor gets an empty answer rather than somebody else's task. The lease owner is the
 * executor's {@code instance_id}: when the process dies, the lease expires and the recovery path
 * takes the attempt back without the registry having to notice (PRD 8.3).
 *
 * <p>Each claim is its own transaction: a race lost on one instance must not roll back the claims
 * already handed to this executor.
 */
@Profile("!executor")
@Service
public class ExecutorTaskService {

    private final ExecutorRegistryService registryService;
    private final TaskOperations taskOperations;
    private final ExecutorProperties properties;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public ExecutorTaskService(ExecutorRegistryService registryService, TaskOperations taskOperations,
                               ExecutorProperties properties, ObjectMapper objectMapper, Clock clock) {
        this.registryService = registryService;
        this.taskOperations = taskOperations;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /**
     * Hands the executor up to {@code batchSize} claimable instances of its group.
     *
     * <p>The executor must be ONLINE - a stale heartbeat means the center believes the process is
     * gone and will not route work to it. An executor that was wrongly swept can re-register and
     * claim again; the instances it still holds are recovered through the ordinary lease path.
     *
     * @return the claimed instances, empty when there was nothing to take
     * @throws com.yuki.reactorsched.executor.registry.exception.ExecutorOfflineException when the
     *         executor's heartbeat went stale
     */
    public List<ClaimedTaskView> claimTasks(long executorId, ClaimTasksRequest request) {
        Executor executor = registryService.requireOnline(executorId);
        String workerId = executor.getInstanceId();
        String group = executor.getGroupName();
        LocalDateTime now = LocalDateTime.now(clock);

        List<Task> candidates = taskOperations.findClaimable(now, group, request.effectiveBatchSize());
        List<ClaimedTaskView> claimed = new ArrayList<>(candidates.size());
        for (Task candidate : candidates) {
            TaskClaim claim = taskOperations.claimAndOpen(candidate.getId(), workerId, group, now,
                    properties.getLeaseTtl());
            if (claim != null) {
                claimed.add(toView(claim));
            }
        }
        return claimed;
    }

    private ClaimedTaskView toView(TaskClaim claim) {
        Task task = claim.task();
        TaskExecution execution = claim.execution();
        return new ClaimedTaskView(
                task.getId(),
                execution.getId(),
                execution.getAttempt() == null ? 0 : execution.getAttempt(),
                execution.getFencingToken() == null ? 0L : execution.getFencingToken(),
                task.getTaskType(),
                readPayload(task.getPayload()),
                task.getShardIndex() == null ? 0 : task.getShardIndex(),
                task.getShardTotal() == null ? 1 : task.getShardTotal(),
                task.getScheduledAt(),
                task.getTimeoutMs(),
                task.getLeaseUntil());
    }

    /**
     * Parses the payload snapshot the instance carries. The column is written as JSON and validated
     * at creation, so a parse failure means corruption rather than a client mistake; the raw text is
     * handed over as a JSON string so the handler still sees what was stored instead of {@code null}.
     */
    private JsonNode readPayload(String payload) {
        if (payload == null || payload.isBlank()) {
            return NullNode.getInstance();
        }
        try {
            return objectMapper.readTree(payload);
        } catch (UncheckedIOException | com.fasterxml.jackson.core.JsonProcessingException e) {
            return TextNode.valueOf(payload);
        }
    }
}
