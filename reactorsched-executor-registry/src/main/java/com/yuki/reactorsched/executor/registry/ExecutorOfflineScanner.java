package com.yuki.reactorsched.executor.registry;

import com.yuki.reactorsched.executor.registry.properties.ExecutorProperties;
import com.yuki.reactorsched.executor.registry.service.ExecutorRegistryService;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs {@link ExecutorRegistryService#sweepOffline()} on a fixed schedule (PRD 7.1).
 *
 * <p>Liveness is a heartbeat, not a connection: an executor that stops heartbeating is taken
 * OFFLINE and stops receiving tasks, while the attempts it still holds stay protected by their
 * leases and are recovered through the ordinary lease path. A failed sweep is logged and the
 * schedule keeps running - the same resilience rule as the lease recovery scanner.
 */
public class ExecutorOfflineScanner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ExecutorOfflineScanner.class);

    private final ExecutorRegistryService registryService;
    private final ExecutorProperties properties;

    /** Meters of PRD 8.6; attached after construction, absent in unit tests. */
    private volatile ReactorMetrics metrics;

    private ScheduledExecutorService scheduler;
    private volatile boolean running;
    private volatile int lastSwept;

    public ExecutorOfflineScanner(ExecutorRegistryService registryService, ExecutorProperties properties) {
        this.registryService = registryService;
        this.properties = properties;
        validate(properties);
    }

    /** Attaches the meters; without them the sweep behaves identically (PRD 8.6). */
    public void attachMetrics(ReactorMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "reactorsched-executor-sweep");
            thread.setDaemon(false);
            return thread;
        });
        running = true;
        long interval = properties.sweepIntervalMillis();
        scheduler.scheduleWithFixedDelay(this::sweepQuietly, 0, interval, TimeUnit.MILLISECONDS);
        log.info("executor offline sweep started: sweepInterval={}ms offlineThreshold={}s",
                interval, properties.effectiveOfflineThreshold().toSeconds());
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
                log.warn("executor offline sweep did not stop within 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("executor offline sweep stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** One sweep, used by the schedule and by tests that want a deterministic pass. */
    public int sweepOnce() {
        int swept = registryService.sweepOffline();
        lastSwept = swept;
        if (swept > 0) {
            log.info("executor offline sweep took {} executor(s) OFFLINE", swept);
        }
        publishGaugesQuietly();
        return swept;
    }

    /** How many executors the last sweep took offline. */
    public int getLastSwept() {
        return lastSwept;
    }

    private void sweepQuietly() {
        try {
            sweepOnce();
        } catch (RuntimeException e) {
            log.warn("executor offline sweep failed, keeping the schedule alive: {}", e.toString());
        }
    }

    /** The gauges follow the sweep: a failure here must not stop the liveness scanning. */
    private void publishGaugesQuietly() {
        if (metrics == null) {
            return;
        }
        try {
            registryService.publishExecutorGauges(metrics);
        } catch (RuntimeException e) {
            log.warn("could not publish the executor gauges: {}", e.toString());
        }
    }

    private static void validate(ExecutorProperties properties) {
        requirePositive(properties.getHeartbeatInterval(), "reactorsched.executor.heartbeat-interval");
        Duration offlineThreshold = properties.getOfflineThreshold();
        if (offlineThreshold != null) {
            requirePositive(offlineThreshold, "reactorsched.executor.offline-threshold");
        }
        Duration sweepInterval = properties.getSweepInterval();
        if (sweepInterval != null) {
            requirePositive(sweepInterval, "reactorsched.executor.sweep-interval");
        }
        requirePositive(properties.getLeaseTtl(), "reactorsched.executor.lease-ttl");
        if (properties.getLogMaxBytes() < 1) {
            throw new IllegalArgumentException("reactorsched.executor.log-max-bytes must be at least 1");
        }
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
