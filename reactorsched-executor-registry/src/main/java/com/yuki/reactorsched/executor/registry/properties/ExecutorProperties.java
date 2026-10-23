package com.yuki.reactorsched.executor.registry.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration of the scheduling center's executor-facing side (prefix
 * {@code reactorsched.executor}, PRD 7.1, 7.2).
 *
 * <p>These values are read by the scheduling center. An executor process configures its own side of
 * the same contract through the SDK's {@code reactorsched.executor.*} properties.
 */
@Getter
@Setter
@ConfigurationProperties("reactorsched.executor")
public class ExecutorProperties {

    /** Shared secret executors present as {@code X-ReactorSched-Token} (PRD 10.3). */
    private String token = "";

    /** Expected heartbeat interval; the offline threshold defaults to three missed heartbeats. */
    private Duration heartbeatInterval = Duration.ofSeconds(10);

    /** How long a heartbeat may be missing before the executor is swept OFFLINE. */
    private Duration offlineThreshold;

    /** How often the offline sweep runs. Defaults to the heartbeat interval. */
    private Duration sweepInterval;

    /** Lease an executor takes when it claims an instance (PRD 7.2). */
    private Duration leaseTtl = Duration.ofSeconds(60);

    /** Upper bound of the log text accepted with a result, beyond what the SDK already bounded. */
    private int logMaxBytes = 64 * 1024;

    public Duration effectiveOfflineThreshold() {
        return offlineThreshold == null ? heartbeatInterval.multipliedBy(3) : offlineThreshold;
    }

    public Duration effectiveSweepInterval() {
        if (sweepInterval != null) {
            return sweepInterval;
        }
        return heartbeatInterval;
    }

    public long sweepIntervalMillis() {
        return Math.max(1L, effectiveSweepInterval().toMillis());
    }
}
