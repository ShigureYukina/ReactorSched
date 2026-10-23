package com.yuki.reactorsched.scheduler;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Reports the scheduler's health: the node clock versus the database clock (PRD 6.5) and the
 * outcome of the last scan.
 *
 * <p>Clock drift beyond {@code reactorsched.scheduler.max-clock-skew} makes the node's fire time
 * decisions wrong, so it is a {@code DOWN} rather than a detail: an operator seeing it should fix
 * the clock, not wonder why instances fire early.
 */
public class SchedulerClockHealthIndicator implements HealthIndicator {

    private final DatabaseClockProbe clockProbe;
    private final SchedulerProperties properties;
    private final ScheduleScanner scanner;

    public SchedulerClockHealthIndicator(DatabaseClockProbe clockProbe,
                                         SchedulerProperties properties,
                                         ScheduleScanner scanner) {
        this.clockProbe = clockProbe;
        this.properties = properties;
        this.scanner = scanner;
    }

    @Override
    public Health health() {
        ScheduleScanResult last = scanner.getLastResult();
        try {
            DatabaseClockProbe.Measurement measurement = clockProbe.measure();
            Health.Builder builder = measurement.exceeds(properties.effectiveMaxClockSkew())
                    ? Health.down()
                    : Health.up();
            return builder
                    .withDetail("databaseNow", measurement.databaseNow().toString())
                    .withDetail("nodeNow", measurement.nodeNow().toString())
                    .withDetail("skewMillis", measurement.skewMillis())
                    .withDetail("maxClockSkewMillis", properties.effectiveMaxClockSkew().toMillis())
                    .withDetail("jobsScanned", last.jobsScanned())
                    .withDetail("fireTimesFired", last.fireTimesFired())
                    .withDetail("misfireDecisions", last.misfireDecisions())
                    .withDetail("triggerFailures", last.triggerFailures())
                    .build();
        } catch (RuntimeException e) {
            // Without the database clock there is no health answer at all, and without a database
            // the scheduler cannot fire anything: that is DOWN.
            return Health.down(e)
                    .withDetail("reason", "database clock unavailable")
                    .build();
        }
    }
}
