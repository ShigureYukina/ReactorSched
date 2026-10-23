package com.yuki.reactorsched.core.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the meter names, tags and histogram behaviour of PRD 8.6: a renamed or untagged meter would
 * silently empty a Grafana panel, and a timer without buckets would turn P99 into a lie.
 */
class ReactorMetricsTest {

    private SimpleMeterRegistry registry;
    private ReactorMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new ReactorMetrics(registry);
    }

    @Test
    void declaresTheGroupsMetersSoAnIdleSystemReportsZeros() {
        metrics.declareGroup("default");

        assertThat(registry.find("reactorsched.executor.heartbeat.age")
                .tag("executor_group", "default").gauge()).isNotNull();
        assertThat(registry.find("reactorsched.executor.online")
                .tag("executor_group", "default").gauge()).isNotNull();
        assertThat(registry.find("reactorsched.concurrency.limit")
                .tag("executor_group", "default").gauge()).isNotNull();
        // -1 marks "no adaptive controller has published a limit yet" and must not read as 0.
        assertThat(registry.find("reactorsched.concurrency.limit")
                .tag("executor_group", "default").gauge().value()).isEqualTo(-1.0);
        metrics.declareGroup("default");
        assertThat(registry.getMeters()).allMatch(meter -> true);
        assertThat(registry.find("reactorsched.executor.online").gauges()).hasSize(1);
    }

    @Test
    void countsCompletionsUnderTheirResultAndTypeTags() {
        metrics.taskCompleted("SUCCEEDED", "downstream", "default");
        metrics.taskCompleted("FAILED", "downstream", "default");
        metrics.taskCompleted("FAILED", "downstream", "default");

        assertThat(counterValue("reactorsched.task.completed", "result", "SUCCEEDED",
                "task_type", "downstream", "executor_group", "default")).isEqualTo(1.0);
        assertThat(counterValue("reactorsched.task.completed", "result", "FAILED",
                "task_type", "downstream", "executor_group", "default")).isEqualTo(2.0);
    }

    @Test
    void executionDurationTimerPublishesBucketsForP99() {
        metrics.executionDuration("downstream", "SUCCEEDED", 120);

        Timer timer = registry.find("reactorsched.task.execution.duration")
                .tag("task_type", "downstream")
                .tag("result", "SUCCEEDED")
                .timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.max(TimeUnit.MILLISECONDS)).isEqualTo(120.0);
        // Without buckets histogram_quantile cannot be computed; this is the PRD 8.6 requirement.
        assertThat(timer.takeSnapshot().histogramCounts()).isNotEmpty();
    }

    @Test
    void queueWaitTimerSeparatesFirstAttemptsFromRetries() {
        metrics.queueWait(true, 10);
        metrics.queueWait(false, 20);

        assertThat(registry.find("reactorsched.task.queue.wait")
                .tag("first_attempt", "true").timer().count()).isEqualTo(1);
        assertThat(registry.find("reactorsched.task.queue.wait")
                .tag("first_attempt", "false").timer().count()).isEqualTo(1);
    }

    @Test
    void jobTriggerCounterCarriesScheduleTypeAndOutcome() {
        metrics.jobTriggered("ONE_TIME", "fired");
        metrics.jobTriggered("CRON", "failed");

        assertThat(counterValue("reactorsched.job.trigger", "schedule_type", "ONE_TIME", "outcome", "fired"))
                .isEqualTo(1.0);
        assertThat(counterValue("reactorsched.job.trigger", "schedule_type", "CRON", "outcome", "failed"))
                .isEqualTo(1.0);
    }

    @Test
    void groupGaugesKeepTheirValueBetweenUpdates() {
        metrics.declareGroup("default");
        metrics.executorsOnline("default", 3);
        metrics.heartbeatAge("default", 7);

        Gauge online = registry.find("reactorsched.executor.online")
                .tag("executor_group", "default").gauge();
        Gauge age = registry.find("reactorsched.executor.heartbeat.age")
                .tag("executor_group", "default").gauge();
        assertThat(online.value()).isEqualTo(3.0);
        assertThat(age.value()).isEqualTo(7.0);

        // An unknown group has no series: publishing into it must be a no-op, not a new meter.
        metrics.executorsOnline("ghost", 5);
        assertThat(registry.find("reactorsched.executor.online")
                .tag("executor_group", "ghost").gauge()).isNull();
    }

    @Test
    void concurrencyLimitPublishesCurrentAndMaxForTheGroup() {
        metrics.concurrencyLimit("default", 2.0, 8.0);

        assertThat(registry.find("reactorsched.concurrency.limit")
                .tag("executor_group", "default").gauge().value()).isEqualTo(2.0);
        assertThat(registry.find("reactorsched.concurrency.max")
                .tag("executor_group", "default").gauge().value()).isEqualTo(8.0);

        metrics.concurrencyLimit("default", 4.0, 8.0);
        assertThat(registry.find("reactorsched.concurrency.limit")
                .tag("executor_group", "default").gauge().value()).isEqualTo(4.0);
    }

    @Test
    void backlogGaugesAreGlobalBecauseTheDatabaseIs() {
        metrics.backlog(12, 5);

        assertThat(registry.find("reactorsched.backlog.due").gauge().value()).isEqualTo(12.0);
        assertThat(registry.find("reactorsched.running").gauge().value()).isEqualTo(5.0);
        // No executor_group tag: one database, one number, whatever scrapes it.
        assertThat(registry.find("reactorsched.backlog.due").gauge().getId().getTag("executor_group")).isNull();
    }

    @Test
    void decisionCounterSplitsByTypeAndReason() {
        metrics.decisionRecorded("TASK_DEFERRAL", "CAPACITY_LIMIT");
        metrics.decisionRecorded("TASK_DEFERRAL", "CAPACITY_LIMIT");
        metrics.decisionRecorded("RETRY_SCHEDULED", "RETRY_BACKOFF");

        assertThat(counterValue("reactorsched.decision.recorded", "decision_type", "TASK_DEFERRAL",
                "reason_code", "CAPACITY_LIMIT")).isEqualTo(2.0);
        assertThat(counterValue("reactorsched.decision.recorded", "decision_type", "RETRY_SCHEDULED",
                "reason_code", "RETRY_BACKOFF")).isEqualTo(1.0);
    }

    private double counterValue(String name, String... tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        assertThat(counter).as("counter %s with tags %s", name, tags).isNotNull();
        return counter.count();
    }
}
