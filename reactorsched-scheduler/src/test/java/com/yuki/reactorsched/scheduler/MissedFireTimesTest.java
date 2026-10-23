package com.yuki.reactorsched.scheduler;

import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.MisfirePolicy;
import com.yuki.reactorsched.core.domain.ScheduleType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Arithmetic over missed fire times, driven by a table for the cron cases so an edge case is a data
 * row rather than prose (PRD 6.4, AGENTS.md 12).
 *
 * <p>All times are UTC, matching how the scheduler stores and compares them.
 */
class MissedFireTimesTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 10, 22, 12, 30, 0);

    @Test
    void fixedRateIsExactForAnyDowntime() {
        JobDefinition job = fixedRate(60);
        LocalDateTime from = NOW.minusSeconds(610);
        // Horizon is NOW (grace 0): 11 occurrences are expired, the 12th is the first fresh one.
        assertThat(MissedFireTimes.countMissed(job, from, NOW)).isEqualTo(11);
        assertThat(MissedFireTimes.firstNotExpired(job, from, NOW)).isEqualTo(from.plusSeconds(660));
        assertThat(MissedFireTimes.mostRecentMissed(job, from, NOW)).isEqualTo(from.plusSeconds(600));
    }

    @Test
    void fixedRateCountsAgainstTheGraceHorizonNotNow() {
        JobDefinition job = fixedRate(60);
        job.setMisfireGraceSeconds(300);
        LocalDateTime from = NOW.minusSeconds(610);
        // Horizon is NOW - 300s: 6 occurrences are expired, the 7th is the first fresh one.
        assertThat(MissedFireTimes.countMissed(job, from, NOW)).isEqualTo(6);
        assertThat(MissedFireTimes.firstNotExpired(job, from, NOW)).isEqualTo(from.plusSeconds(360));
        assertThat(MissedFireTimes.mostRecentMissed(job, from, NOW)).isEqualTo(from.plusSeconds(300));
    }

    @Test
    void fixedRateOnAStepBoundaryDoesNotDrift() {
        JobDefinition job = fixedRate(60);
        LocalDateTime from = NOW.minusSeconds(600);
        // Exactly on the boundary: the occurrence at NOW is fresh, not expired.
        assertThat(MissedFireTimes.countMissed(job, from, NOW)).isEqualTo(10);
        assertThat(MissedFireTimes.firstNotExpired(job, from, NOW)).isEqualTo(NOW);
    }

    @Test
    void oneOffHasOneFireTimeAndNothingFollows() {
        JobDefinition job = oneOff(NOW.minusSeconds(600));
        assertThat(MissedFireTimes.firstNotExpired(job, NOW.minusSeconds(600), NOW)).isNull();
        assertThat(MissedFireTimes.countMissed(job, NOW.minusSeconds(600), NOW)).isEqualTo(1);
        assertThat(MissedFireTimes.mostRecentMissed(job, NOW.minusSeconds(600), NOW))
                .isEqualTo(NOW.minusSeconds(600));
        // Inside the grace period there is nothing to miss.
        job.setMisfireGraceSeconds(900);
        assertThat(MissedFireTimes.countMissed(job, NOW.minusSeconds(600), NOW)).isZero();
    }

    @Test
    void cronTable() {
        // cron | from | grace | expected first fresh | expected count | expected most recent
        Object[][] cases = {
                // Per-minute cron, ten minutes down: the next whole minute is where the plan resumes.
                {"0 * * * * *", LocalDateTime.of(2026, 10, 22, 12, 0), 0,
                        LocalDateTime.of(2026, 10, 22, 12, 30), 30, LocalDateTime.of(2026, 10, 22, 12, 29)},
                // Same cron with a grace period: the resume point moves back by the grace.
                {"0 * * * * *", LocalDateTime.of(2026, 10, 22, 12, 0), 300,
                        LocalDateTime.of(2026, 10, 22, 12, 25), 25, LocalDateTime.of(2026, 10, 22, 12, 24)},
                // Daily cron: the parked occurrence and today's are both inside the horizon, the
                // next one is tomorrow.
                {"0 0 2 * * *", LocalDateTime.of(2026, 10, 21, 2, 0), 0,
                        LocalDateTime.of(2026, 10, 23, 2, 0), 2, LocalDateTime.of(2026, 10, 22, 2, 0)},
                // Month end: the resume point skips the days February does not have.
                {"0 0 0 1 * *", LocalDateTime.of(2026, 1, 15, 0, 0), 0,
                        LocalDateTime.of(2026, 11, 1, 0, 0), 10, LocalDateTime.of(2026, 10, 1, 0, 0)},
                // Leap year: 2028-02-29 is the occurrence that resumes the plan, and until then the
                // parked fire time itself is the most recent missed one.
                {"0 0 0 29 2 *", LocalDateTime.of(2026, 3, 1, 0, 0), 0,
                        LocalDateTime.of(2028, 2, 29, 0, 0), 1, LocalDateTime.of(2026, 3, 1, 0, 0)},
        };
        for (Object[] row : cases) {
            JobDefinition job = cron((String) row[0]);
            job.setMisfireGraceSeconds((Integer) row[2]);
            LocalDateTime from = (LocalDateTime) row[1];
            String label = "cron " + row[0] + " from " + from + " grace " + row[2];
            assertThat(MissedFireTimes.firstNotExpired(job, from, NOW))
                    .as(label + " first fresh").isEqualTo(row[3]);
            assertThat(MissedFireTimes.countMissed(job, from, NOW))
                    .as(label + " count").isEqualTo(row[4]);
            assertThat(MissedFireTimes.mostRecentMissed(job, from, NOW))
                    .as(label + " most recent").isEqualTo(row[5]);
        }
    }

    @Test
    void cronHorizonRoundsUpToTheWholeSecond() {
        JobDefinition job = cron("0 * * * * *");
        // A horizon with milliseconds: occurrences sit on whole seconds, so 12:30:00.500 rounds up
        // to 12:30:01 and the first fresh occurrence stays 12:31:00, not 12:30:00.
        LocalDateTime now = NOW.plusNanos(500_000_000);
        LocalDateTime from = LocalDateTime.of(2026, 10, 22, 12, 0);
        assertThat(MissedFireTimes.firstNotExpired(job, from, now))
                .isEqualTo(LocalDateTime.of(2026, 10, 22, 12, 31));
    }

    @Test
    void perSecondCronWalkIsBounded() {
        JobDefinition job = cron("* * * * * *");
        LocalDateTime from = NOW.minusSeconds(20_000);
        // The walk stops at the bound instead of enumerating twenty thousand occurrences; the
        // reported count is the bound, not the truth, and the resume point is still a fresh one.
        assertThat(MissedFireTimes.countMissed(job, from, NOW)).isEqualTo(MissedFireTimes.MAX_MISSED_SCAN);
        assertThat(MissedFireTimes.firstNotExpired(job, from, NOW)).isEqualTo(NOW);
    }

    @Test
    void nothingIsMissedWhenTheFireTimeIsStillFresh() {
        JobDefinition job = fixedRate(60);
        job.setMisfireGraceSeconds(300);
        LocalDateTime from = NOW.minusSeconds(299);
        assertThat(MissedFireTimes.countMissed(job, from, NOW)).isZero();
        // Nothing expired, so the plan resumes at the fire time it is already parked on.
        assertThat(MissedFireTimes.firstNotExpired(job, from, NOW)).isEqualTo(from);
        assertThat(MissedFireTimes.mostRecentMissed(job, from, NOW)).isEqualTo(from);
    }

    private static JobDefinition cron(String expression) {
        JobDefinition job = new JobDefinition();
        job.setId(1L);
        job.setJobKey("cron");
        job.setScheduleType(ScheduleType.CRON);
        job.setCronExpr(expression);
        job.setMisfirePolicy(MisfirePolicy.SKIP);
        return job;
    }

    private static JobDefinition fixedRate(int seconds) {
        JobDefinition job = new JobDefinition();
        job.setId(1L);
        job.setJobKey("fixed");
        job.setScheduleType(ScheduleType.FIXED_RATE);
        job.setIntervalSeconds(seconds);
        job.setMisfirePolicy(MisfirePolicy.SKIP);
        return job;
    }

    private static JobDefinition oneOff(LocalDateTime runAt) {
        JobDefinition job = new JobDefinition();
        job.setId(1L);
        job.setJobKey("one-off");
        job.setScheduleType(ScheduleType.ONE_TIME);
        job.setRunAt(runAt);
        job.setMisfirePolicy(MisfirePolicy.SKIP);
        return job;
    }
}
