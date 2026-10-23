package com.yuki.reactorsched.core.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The Micrometer meters of PRD 8.6, registered under their documented names.
 *
 * <p>Both latency timers publish histograms. P99 comes from {@code histogram_quantile} over the
 * buckets, never from a mean: the mean of a bimodal distribution says nothing about the tail an
 * SLO is about (PRD 8.6).
 *
 * <p>Group-tagged meters (completed counter, heartbeat age, online executors, concurrency) go
 * through {@link GroupGaugeRegistry}, which registers one gauge per executor group on first sight
 * and keeps it afterwards. A group that stops reporting keeps its last value instead of vanishing
 * from the dashboard, which makes "stopped" distinguishable from "never existed".
 *
 * <p>The two global gauges (due backlog, running attempts) are refreshed periodically by
 * {@link BacklogGaugeRefresher} rather than counted in-process: several scheduler and executor
 * instances share one database, so an in-process counter would under-report every series except
 * the writer's own.
 */
public final class ReactorMetrics {

    private final MeterRegistry registry;
    private final GroupGaugeRegistry groupGauges;

    /** Per executor group: seconds since the group's most recent heartbeat. */
    private final ConcurrentHashMap<String, Holder> heartbeatAge = new ConcurrentHashMap<>();
    /** Per executor group: how many executors of that group are ONLINE. */
    private final ConcurrentHashMap<String, Holder> executorsOnline = new ConcurrentHashMap<>();
    /** Due backlog (PENDING/RETRY_WAIT and eligible), refreshed periodically. */
    private Holder backlogDue;
    /** Attempts currently RUNNING, refreshed periodically. */
    private Holder running;

    public ReactorMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.groupGauges = new GroupGaugeRegistry(registry);
        // The holders come back from the registry so the gauges read the same object backlog()
        // updates; a separate instance would freeze at zero.
        this.backlogDue = groupGauges.gauge("reactorsched.backlog.due", "", 0.0);
        this.running = groupGauges.gauge("reactorsched.running", "", 0.0);
    }

    /**
     * Creates the group's meter series before its first event, so a scrape right after startup
     * reports zeros instead of "no data": an idle system must be distinguishable from a broken one.
     * Repeat calls for the same group are no-ops.
     */
    public void declareGroup(String group) {
        heartbeatAge.computeIfAbsent(group,
                key -> groupGauges.gauge("reactorsched.executor.heartbeat.age", key, 0.0));
        executorsOnline.computeIfAbsent(group,
                key -> groupGauges.gauge("reactorsched.executor.online", key, 0.0));
        concurrencyLimit(group, -1.0, -1.0);
    }

    public void taskCompleted(String result, String taskType, String executorGroup) {
        // register() finds an already registered meter with the same name and tags, so the first
        // call creates the series and later ones reuse it.
        Counter.builder("reactorsched.task.completed")
                .description("Attempts that reached a terminal outcome")
                .tags("result", tag(result), "task_type", tag(taskType), "executor_group", tag(executorGroup))
                .register(registry)
                .increment();
    }

    /** Latency buckets in milliseconds, covering a fast in-memory handler up to a slow downstream. */
    private static final Duration[] LATENCY_SLOS = {
            Duration.ofMillis(10), Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(250),
            Duration.ofMillis(500), Duration.ofMillis(1_000), Duration.ofMillis(2_500),
            Duration.ofMillis(5_000), Duration.ofMillis(10_000), Duration.ofMillis(30_000)
    };

    public void executionDuration(String taskType, String result, long millis) {
        // Built through the builder on every call on purpose: withTags() would not carry the
        // histogram configuration, and P99 depends on the buckets.
        Timer.builder("reactorsched.task.execution.duration")
                .description("Handler execution latency; P99 comes from the histogram buckets")
                .publishPercentileHistogram()
                .serviceLevelObjectives(LATENCY_SLOS)
                .tags("task_type", tag(taskType), "result", tag(result))
                .register(registry)
                .record(Math.max(0L, millis), TimeUnit.MILLISECONDS);
    }

    public void queueWait(boolean firstAttempt, long millis) {
        Timer.builder("reactorsched.task.queue.wait")
                .description("Time between the instance becoming eligible and its claim")
                .publishPercentileHistogram()
                .serviceLevelObjectives(LATENCY_SLOS)
                .tags("first_attempt", Boolean.toString(firstAttempt))
                .register(registry)
                .record(Math.max(0L, millis), TimeUnit.MILLISECONDS);
    }

    public void jobTriggered(String scheduleType, String outcome) {
        Counter.builder("reactorsched.job.trigger")
                .description("Fire times a definition turned into instances, or failed to")
                .tags("schedule_type", tag(scheduleType), "outcome", tag(outcome))
                .register(registry)
                .increment();
    }

    public void heartbeatAge(String executorGroup, long seconds) {
        Holder holder = heartbeatAge.get(executorGroup);
        if (holder != null) {
            holder.set(seconds);
        }
    }

    public void executorsOnline(String executorGroup, long count) {
        Holder holder = executorsOnline.get(executorGroup);
        if (holder != null) {
            holder.set(count);
        }
    }

    /** Publishes the group's current concurrency limit and its upper bound (PRD 8.6). */
    public void concurrencyLimit(String executorGroup, double current, double max) {
        Holder currentHolder = groupGauges.gauge("reactorsched.concurrency.limit", executorGroup, current);
        currentHolder.set(current);
        groupGauges.gauge("reactorsched.concurrency.max", executorGroup, max).set(max);
    }

    public void concurrencyAdjusted(String executorGroup) {
        Counter.builder("reactorsched.concurrency.adjustments")
                .description("Applied AIMD concurrency adjustments")
                .tags("executor_group", tag(executorGroup))
                .register(registry)
                .increment();
    }

    /**
     * One decision row written by the scheduling code paths (PRD 8.7). This counter is the metric
     * behind the "decision reason distribution" panel that PRD 8.6 requires: the rows themselves
     * live in {@code scheduler_decision} and are deduplicated there (one row per instance and
     * reason per attempt), so the counter stays low-frequency by construction and its tags are
     * the two closed enums - no high-cardinality label is involved.
     */
    public void decisionRecorded(String decisionType, String reasonCode) {
        Counter.builder("reactorsched.decision.recorded")
                .description("Scheduling decisions written, by type and reason")
                .tags("decision_type", tag(decisionType), "reason_code", tag(reasonCode))
                .register(registry)
                .increment();
    }

    public void backlog(long due, long runningCount) {
        backlogDue.set(due);
        running.set(runningCount);
    }

    private static String tag(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }

    /** A gauge value that survives between scrapes; see the class comment. */
    static final class Holder {
        private volatile double value;

        void set(double next) {
            this.value = next;
        }

        double value() {
            return value;
        }
    }
}
