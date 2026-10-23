package com.yuki.reactorsched.core.backpressure;

import com.yuki.reactorsched.core.decision.SchedulerDecisionRecorder;
import com.yuki.reactorsched.core.exception.BackpressureRejectedException;
import com.yuki.reactorsched.core.mapper.TaskMapper;

import java.time.Clock;
import java.time.LocalDateTime;

/**
 * Admission check of the entry points (PRD 8.5): reads the due backlog and the waiting queue,
 * writes a {@code BACKPRESSURE_REJECTED} decision when a threshold is breached, and only then
 * raises. The decision row is written before the exception so the rejection is explainable even
 * though the caller never reaches the service method.
 *
 * <p>Callers run this outside a transaction (the controllers call it before the service call), so
 * the decision row commits on its own and cannot be rolled back together with the refused work.
 */
public class BackpressureGuard {

    private final TaskMapper taskMapper;
    private final BackpressureProperties properties;
    private final SchedulerDecisionRecorder decisionRecorder;
    private final Clock clock;

    public BackpressureGuard(TaskMapper taskMapper, BackpressureProperties properties,
                             SchedulerDecisionRecorder decisionRecorder, Clock clock) {
        this.taskMapper = taskMapper;
        this.properties = properties;
        this.decisionRecorder = decisionRecorder;
        this.clock = clock;
    }

    /**
     * @param entryPoint human-readable name of the admission point, e.g. {@code "task submission"}
     * @throws BackpressureRejectedException when either metric exceeds its threshold
     */
    public void checkAdmission(String entryPoint) {
        LocalDateTime now = LocalDateTime.now(clock);
        long dueBacklog = taskMapper.countDueBacklog(now);
        long waitingQueue = taskMapper.countBacklog();
        long backlogThreshold = properties.getBacklogRejectThreshold();
        long queueThreshold = properties.getQueueRejectThreshold();
        boolean backlogBreach = backlogThreshold > 0 && dueBacklog > backlogThreshold;
        boolean queueBreach = queueThreshold > 0 && waitingQueue > queueThreshold;
        if (!backlogBreach && !queueBreach) {
            return;
        }
        decisionRecorder.recordBackpressureRejected(entryPoint, dueBacklog, waitingQueue,
                backlogThreshold, queueThreshold, now);
        throw new BackpressureRejectedException(entryPoint, dueBacklog, waitingQueue);
    }
}
