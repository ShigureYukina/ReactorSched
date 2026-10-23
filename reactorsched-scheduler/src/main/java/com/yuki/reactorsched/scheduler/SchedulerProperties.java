package com.yuki.reactorsched.scheduler;

import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.ScheduleType;
import com.yuki.reactorsched.core.schedule.ScheduleCalculator;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration of the scheduler (prefix {@code reactorsched.scheduler}, PRD 6.2, 6.4, 6.5).
 *
 * <p>Values are validated when the scanner is constructed so a misconfigured scheduler fails at
 * startup instead of silently never firing anything.
 */
@Getter
@Setter
@ConfigurationProperties("reactorsched.scheduler")
public class SchedulerProperties {

    /** Whether this process runs a scheduler at all. A process that only serves the API sets false. */
    private boolean enabled = true;

    /** Delay between two scans of the due definitions (PRD 6.2 default: 1s). */
    private Duration scanInterval = Duration.ofSeconds(1);

    /** Maximum number of definitions one scan claims fire times for. */
    private int batchSize = 50;

    /** Maximum number of missed fire times one catch-up round creates instances for (PRD 6.4 default: 20). */
    private int maxCatchUp = 20;

    /** How far the node clock may drift from the database clock before it is reported (PRD 6.5 default: 2s). */
    private Duration maxClockSkew = Duration.ofSeconds(2);

    public long scanIntervalMillis() {
        return Math.max(1L, scanInterval.toMillis());
    }

    public int effectiveBatchSize() {
        return Math.max(1, batchSize);
    }

    public int effectiveMaxCatchUp() {
        return Math.max(1, maxCatchUp);
    }

    public Duration effectiveMaxClockSkew() {
        return maxClockSkew == null || maxClockSkew.isNegative() ? Duration.ZERO : maxClockSkew;
    }
}
