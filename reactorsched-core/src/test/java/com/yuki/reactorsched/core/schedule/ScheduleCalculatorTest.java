package com.yuki.reactorsched.core.schedule;

import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.MisfirePolicy;
import com.yuki.reactorsched.core.domain.ScheduleType;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schedule arithmetic driven by a table, so a cron edge case is a data row rather than a paragraph of
 * prose (PRD 13.1, AGENTS.md 12).
 *
 * <p>All times are UTC, matching how the scheduler stores and compares them.
 */
class ScheduleCalculatorTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 10, 22, 12, 30, 0);

    @Test
    void cronNextOccurrenceTable() {
        // expression | from | expected next
        Object[][] cases = {
                {"0 0 2 * * *", LocalDateTime.of(2026, 10, 22, 1, 0), LocalDateTime.of(2026, 10, 22, 2, 0)},
                {"0 0 2 * * *", LocalDateTime.of(2026, 10, 22, 2, 0), LocalDateTime.of(2026, 10, 23, 2, 0)},
                {"*/15 * * * * *", LocalDateTime.of(2026, 10, 22, 12, 30, 0), LocalDateTime.of(2026, 10, 22, 12, 30, 15)},
                {"0 0 0 1 * *", LocalDateTime.of(2026, 10, 22, 12, 30), LocalDateTime.of(2026, 11, 1, 0, 0)},
                // Month end: 31 January -> 1 February (there is no 31 February).
                {"0 0 0 31 * *", LocalDateTime.of(2029, 1, 15, 0, 0), LocalDateTime.of(2029, 1, 31, 0, 0)},
                // Leap year: 2028-02-29 exists, 2027 has no 29 February.
                {"0 0 0 29 2 *", LocalDateTime.of(2027, 3, 1, 0, 0), LocalDateTime.of(2028, 2, 29, 0, 0)},
                // Year boundary.
                {"0 0 0 1 1 *", LocalDateTime.of(2026, 12, 31, 23, 59), LocalDateTime.of(2027, 1, 1, 0, 0)},
        };
        for (Object[] row : cases) {
            JobDefinition job = cronJob((String) row[0]);
            assertThat(ScheduleCalculator.firstFireAtOrAfter(job, (LocalDateTime) row[1]))
                    .as("cron %s from %s", row[0], row[1])
                    .isEqualTo(row[2]);
        }
    }

    @Test
    void fixedRateAdvancesFromThePlannedTimeNotFromNow() {
        JobDefinition job = fixedRateJob(30);
        LocalDateTime planned = NOW;
        assertThat(ScheduleCalculator.nextFireAtAfter(job, planned)).isEqualTo(planned.plusSeconds(30));
        // A scheduler that woke up three minutes late must not shift the cadence.
        assertThat(ScheduleCalculator.nextFireAtAfter(job, planned)).isEqualTo(planned.plusSeconds(30));
    }

    @Test
    void oneOffHasNoNextFireTime() {
        JobDefinition job = new JobDefinition();
        job.setScheduleType(ScheduleType.ONE_TIME);
        job.setRunAt(NOW.plusSeconds(10));
        assertThat(ScheduleCalculator.nextFireAtAfter(job, job.getRunAt())).isNull();
    }

    @Test
    void misfireUsesTheGracePeriodInclusively() {
        JobDefinition job = fixedRateJob(60);
        job.setMisfireGraceSeconds(300);
        LocalDateTime planned = NOW.minusSeconds(300);
        assertThat(ScheduleCalculator.isMisfire(job, planned, NOW)).isFalse();
        assertThat(ScheduleCalculator.isMisfire(job, planned.minusSeconds(1), NOW)).isTrue();
    }

    @Test
    void catchUpStopsAtItsLimit() {
        JobDefinition job = fixedRateJob(60);
        job.setMisfireGraceSeconds(0);
        Clock clock = Clock.fixed(Instant.parse("2026-10-22T12:30:00Z"), ZoneOffset.UTC);
        // 10 minutes late, one minute apart: 10 missed fire times, capped at 3.
        List<LocalDateTime> missed = ScheduleCalculator.missedFireTimes(job, NOW.minusSeconds(600), 3, clock);
        assertThat(missed).hasSize(3);
        assertThat(missed.get(0)).isEqualTo(NOW.minusSeconds(600));
        assertThat(missed.get(2)).isEqualTo(NOW.minusSeconds(480));
    }

    @Test
    void cronNeedsSixFields() {
        assertThatThrownBy(() -> ScheduleCalculator.parseCron("0 2 * * *"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("six fields");
        assertThatThrownBy(() -> ScheduleCalculator.parseCron("0 0 25 * * *"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void missingMisfirePolicyMeansSkip() {
        JobDefinition job = fixedRateJob(60);
        job.setMisfirePolicy(null);
        assertThat(ScheduleCalculator.misfirePolicyOf(job)).isEqualTo(MisfirePolicy.SKIP);
    }

    private static JobDefinition cronJob(String expression) {
        JobDefinition job = new JobDefinition();
        job.setId(1L);
        job.setJobKey("cron");
        job.setScheduleType(ScheduleType.CRON);
        job.setCronExpr(expression);
        return job;
    }

    private static JobDefinition fixedRateJob(int seconds) {
        JobDefinition job = new JobDefinition();
        job.setId(1L);
        job.setJobKey("fixed");
        job.setScheduleType(ScheduleType.FIXED_RATE);
        job.setIntervalSeconds(seconds);
        return job;
    }
}
