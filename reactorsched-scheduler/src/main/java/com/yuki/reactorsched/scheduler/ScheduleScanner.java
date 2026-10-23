package com.yuki.reactorsched.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs {@link JobSchedulerService#scanOnce()} on a fixed schedule (PRD 6.2).
 *
 * <p>A failed scan is logged and the schedule keeps running: a database that is briefly unavailable
 * must not stop the firing of due definitions.
 *
 * <p>On start the node clock is compared with the database clock once. A node whose clock has
 * drifted decides about fire times with a wrong "now", which is a correctness problem rather than a
 * monitoring one, so a skew beyond {@code reactorsched.scheduler.max-clock-skew} is logged as a
 * warning (PRD 6.5).
 */
public class ScheduleScanner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ScheduleScanner.class);

    private final JobSchedulerService schedulerService;
    private final SchedulerProperties properties;
    private final DatabaseClockProbe clockProbe;

    private ScheduledExecutorService scheduler;
    private volatile boolean running;
    private volatile ScheduleScanResult lastResult = ScheduleScanResult.EMPTY;

    public ScheduleScanner(JobSchedulerService schedulerService,
                           SchedulerProperties properties,
                           DatabaseClockProbe clockProbe) {
        this.schedulerService = schedulerService;
        this.properties = properties;
        this.clockProbe = clockProbe;
        validate(properties);
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        checkClockSkew();
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "reactorsched-scheduler");
            thread.setDaemon(false);
            return thread;
        });
        running = true;
        long interval = properties.scanIntervalMillis();
        scheduler.scheduleWithFixedDelay(this::scanQuietly, 0, interval, TimeUnit.MILLISECONDS);
        log.info("scheduler started: scanInterval={}ms batchSize={} maxCatchUp={}",
                interval, properties.effectiveBatchSize(), properties.effectiveMaxCatchUp());
    }

    @Override
    public void stop() {
        running = false;
        ScheduledExecutorService current = scheduler;
        if (current == null) {
            return;
        }
        current.shutdownNow();
        try {
            if (!current.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("scheduler did not stop within 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("scheduler stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** One scan, used by the schedule and by tests that want a deterministic pass. */
    public ScheduleScanResult scanOnce() {
        ScheduleScanResult result = schedulerService.scanOnce();
        lastResult = result;
        return result;
    }

    public ScheduleScanResult getLastResult() {
        return lastResult;
    }

    private void scanQuietly() {
        try {
            scanOnce();
        } catch (RuntimeException e) {
            log.warn("scheduler scan failed, keeping the schedule alive: {}", e.toString());
        }
    }

    /**
     * Compares the node clock with the database clock. The database is the single source of task
     * state and every fire time decision is made against its rows, so a drifted node fires the
     * wrong instances. The check is advisory: a WARN and nothing else, because a process that
     * refuses to start on skew takes down scheduling entirely, which is the worse failure.
     */
    private void checkClockSkew() {
        try {
            DatabaseClockProbe.Measurement measurement = clockProbe.measure();
            if (measurement.exceeds(properties.effectiveMaxClockSkew())) {
                log.warn("node clock drifted from database clock by {}ms (limit {}ms): "
                                + "fire time decisions on this node may be wrong",
                        measurement.skewMillis(), properties.effectiveMaxClockSkew().toMillis());
            } else {
                log.debug("node clock within {}ms of database clock",
                        measurement.skewMillis());
            }
        } catch (RuntimeException e) {
            log.warn("could not compare node clock with database clock: {}", e.toString());
        }
    }

    private static void validate(SchedulerProperties properties) {
        if (properties.getScanInterval() == null || properties.getScanInterval().isZero()
                || properties.getScanInterval().isNegative()) {
            throw new IllegalArgumentException("reactorsched.scheduler.scan-interval must be positive");
        }
        if (properties.getBatchSize() < 1) {
            throw new IllegalArgumentException("reactorsched.scheduler.batch-size must be at least 1");
        }
        if (properties.getMaxCatchUp() < 1) {
            throw new IllegalArgumentException("reactorsched.scheduler.max-catch-up must be at least 1");
        }
    }
}
