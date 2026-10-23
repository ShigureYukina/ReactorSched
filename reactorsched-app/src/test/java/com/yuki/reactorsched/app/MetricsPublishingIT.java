package com.yuki.reactorsched.app;

import com.yuki.reactorsched.core.domain.ReasonCodes;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import com.yuki.reactorsched.core.retry.RetryProperties;
import com.yuki.reactorsched.worker.TaskWorkerEngine;
import com.yuki.reactorsched.worker.TaskWorkerProperties;
import com.yuki.reactorsched.app.task.TestHandlersConfiguration;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.Search;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The meters of PRD 8.6, observed end to end: a worker runs real tasks and the registry carries the
 * counters, the tagged timers and the group gauges afterwards.
 *
 * <p>The assertions read the meter registry directly instead of scraping {@code /actuator/prometheus}:
 * the scrape format (dots to underscores, the {@code _total} suffix) is Micrometer's concern and is
 * covered by its own tests, while the dashboard queries in {@code deploy/grafana} are written
 * against those exported names.
 */
@Import(TestHandlersConfiguration.class)
class MetricsPublishingIT extends AbstractIntegrationTest {

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private ReactorMetrics reactorMetrics;

    @Autowired
    private TaskMapper taskMapper;

    private static final java.time.Duration TIMEOUT = java.time.Duration.ofSeconds(15);

    @Test
    void publishesCompletionDurationAndQueueWaitForRealTasks() throws Exception {
        TaskWorkerProperties properties = workerProperties(2);
        // The app integration tests share one Spring context, so the meter registry carries the
        // observations of every test class that ran before this one. What this test owns is the
        // delta: the one observation its own task produces.
        double executionBefore = countOf("reactorsched.task.execution.duration",
                "task_type", "scenario", "result", "SUCCEEDED");
        double queueBefore = countOf("reactorsched.task.queue.wait", "first_attempt", "true");

        TaskWorkerEngine worker = startWorker(properties, retryWith(Duration.ofMillis(50), 0.0), null);

        long taskId = submit("scenario", "{}").task().id();
        awaitStatus(taskId, TaskStatus.SUCCEEDED, TIMEOUT);
        worker.stop();

        // Counter under the documented tags.
        assertThat(meterRegistry.find("reactorsched.task.completed")
                .tag("result", "SUCCEEDED")
                .tag("task_type", "scenario")
                .tag("executor_group", properties.getGroup())
                .counter()).isNotNull();
        // Both timers carry one more observation each, with buckets for P99.
        assertThat(countOf("reactorsched.task.execution.duration",
                "task_type", "scenario", "result", "SUCCEEDED")).isEqualTo(executionBefore + 1);
        assertThat(countOf("reactorsched.task.queue.wait", "first_attempt", "true")).isEqualTo(queueBefore + 1);
        assertThat(meterRegistry.find("reactorsched.task.execution.duration")
                .tag("task_type", "scenario")
                .tag("result", "SUCCEEDED")
                .timer().takeSnapshot().histogramCounts()).isNotEmpty();
    }

    /** Observations a tagged timer carries so far; {@code 0} when the meter does not exist yet. */
    private double countOf(String name, String... tags) {
        Search search = meterRegistry.find(name);
        for (int i = 0; i < tags.length; i += 2) {
            search = search.tag(tags[i], tags[i + 1]);
        }
        Timer timer = search.timer();
        return timer == null ? 0.0 : timer.count();
    }

    @Test
    void declaresTheWorkerGroupSoTheDashboardHasSeriesBeforeAnyTask() {
        // The worker's group is declared when the metrics are attached; the gauges must exist with a
        // neutral value rather than appearing only after the first heartbeat sweep.
        reactorMetrics.declareGroup("default");

        assertThat(meterRegistry.find("reactorsched.executor.online")
                .tag("executor_group", "default").gauge()).isNotNull();
        assertThat(meterRegistry.find("reactorsched.backlog.due").gauge()).isNotNull();
        assertThat(meterRegistry.find("reactorsched.running").gauge()).isNotNull();
    }

    @Test
    void backlogGaugesTrackTheDatabase() throws Exception {
        // No worker is running, so the submitted instance stays due and the periodic refresh must
        // pick it up from the database rather than from any in-process counter.
        submit("scenario", "{}");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            reactorMetrics.backlog(taskMapper.countDueBacklog(java.time.LocalDateTime.now()), 0L);
            assertThat(meterRegistry.find("reactorsched.backlog.due").gauge().value()).isPositive();
        });
    }

    @Test
    void decisionCounterIsWiredToTheRecorder() {
        // PRD 8.6 requires a "decision reason distribution" panel; the counter behind it is attached
        // to the recorder by a startup singleton (ReactorCoreConfiguration), so only a running
        // context proves the wiring - a unit test of the recorder alone would pass with the counter
        // silently detached.
        long taskId = submit("scenario", "{}").task().id();
        double before = decisionCount("TASK_DEFERRAL", "CAPACITY_LIMIT");

        schedulerDecisionRecorder.recordTaskDeferral(taskId, 1, ReasonCodes.CAPACITY_LIMIT,
                "worker-under-test", 2, "wiring probe", java.time.LocalDateTime.now(clock));

        assertThat(decisionCount("TASK_DEFERRAL", "CAPACITY_LIMIT")).isEqualTo(before + 1);
    }

    private double decisionCount(String decisionType, String reasonCode) {
        Counter counter = meterRegistry.find("reactorsched.decision.recorded")
                .tag("decision_type", decisionType)
                .tag("reason_code", reasonCode)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }
}
