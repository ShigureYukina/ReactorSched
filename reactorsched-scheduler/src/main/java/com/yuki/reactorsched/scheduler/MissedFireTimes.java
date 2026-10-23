package com.yuki.reactorsched.scheduler;

import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.ScheduleType;
import com.yuki.reactorsched.core.schedule.ScheduleCalculator;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Arithmetic over the fire times a definition has already missed (PRD 6.4).
 *
 * <p>A fire time is <em>expired</em> when {@code now - fireTime > misfireGraceSeconds}; the first
 * fire time that is no longer expired is where a misfire policy resumes the plan. "Not expired"
 * includes times slightly in the past (inside the grace period), so the resumed plan continues from
 * the schedule's own cadence instead of jumping to {@code now}.
 *
 * <p>{@link ScheduleType#ONE_TIME} has exactly one fire time - the parked one - so every question
 * about it has the same trivial answer. {@link ScheduleType#FIXED_RATE} is answered in closed form: its fire times are
 * {@code from + k * interval}, so the first fresh one and the number of missed ones are exact for any
 * downtime. {@link ScheduleType#CRON} has no closed form and no "previous occurrence", so the missed
 * times are walked forward with {@link ScheduleCalculator#parseCron}; the walk is bounded by
 * {@link #MAX_MISSED_SCAN} because a per-second cron that was down for days has millions of
 * occurrences, and a scan must stay cheap. Within that bound the answers are exact.
 */
final class MissedFireTimes {

    /**
     * Upper bound of the forward walk over cron occurrences. A walk that reaches it reports the last
     * occurrence inside the bound rather than the true last one - documented behaviour, not a
     * failure: the plan still resumes at the correct fire time.
     */
    static final int MAX_MISSED_SCAN = 10_000;

    private MissedFireTimes() {
    }

    /**
     * The first fire time of the schedule that is no longer past its grace period, or {@code null}
     * when the schedule has none (a one-off: its single fire time has passed and nothing follows).
     *
     * @param from the fire time the plan is currently parked on (the observed {@code next_fire_at})
     */
    static LocalDateTime firstNotExpired(JobDefinition job, LocalDateTime from, LocalDateTime now) {
        return switch (job.getScheduleType()) {
            case ONE_TIME -> null;
            case CRON -> firstNotExpiredCron(job, graceHorizon(job, now));
            case FIXED_RATE -> from.plusSeconds(stepsUntilNotExpired(job, from, graceHorizon(job, now))
                    * (long) intervalSeconds(job));
        };
    }

    /**
     * The most recent fire time that is past its grace period - the one a {@code FIRE_ONCE} misfire
     * makes up for. {@code from} itself when nothing else was missed.
     */
    static LocalDateTime mostRecentMissed(JobDefinition job, LocalDateTime from, LocalDateTime now) {
        if (job.getScheduleType() == ScheduleType.ONE_TIME) {
            // A one-off has exactly one fire time: itself.
            return from;
        }
        if (job.getScheduleType() == ScheduleType.FIXED_RATE) {
            // The occurrence right before the first fresh one is the last expired one: the first
            // fresh occurrence is the first at or after the horizon, so the one an interval earlier
            // is still before it. When nothing is expired the parked fire time is its own answer.
            LocalDateTime fresh = firstNotExpired(job, from, now);
            return fresh == null || !fresh.isAfter(from) ? from : fresh.minusSeconds(intervalSeconds(job));
        }
        LocalDateTime horizon = graceHorizon(job, now);
        LocalDateTime cursor = from;
        LocalDateTime mostRecent = from;
        for (int walked = 0; walked < MAX_MISSED_SCAN && cursor.isBefore(horizon); walked++) {
            mostRecent = cursor;
            cursor = ScheduleCalculator.parseCron(job.getCronExpr()).next(cursor);
            if (cursor == null) {
                break;
            }
        }
        return mostRecent;
    }

    /**
     * How many fire times between {@code from} and {@code now} are past their grace period. Used for
     * the decision row, which has to say how much of the plan a policy collapsed.
     */
    static int countMissed(JobDefinition job, LocalDateTime from, LocalDateTime now) {
        LocalDateTime horizon = graceHorizon(job, now);
        if (!from.isBefore(horizon)) {
            return 0;
        }
        if (job.getScheduleType() == ScheduleType.ONE_TIME) {
            // A one-off has exactly one fire time, and it is past the horizon here.
            return 1;
        }
        if (job.getScheduleType() == ScheduleType.FIXED_RATE) {
            return stepsUntilNotExpired(job, from, horizon);
        }
        int count = 0;
        LocalDateTime cursor = from;
        while (count < MAX_MISSED_SCAN && cursor.isBefore(horizon)) {
            count++;
            cursor = ScheduleCalculator.parseCron(job.getCronExpr()).next(cursor);
            if (cursor == null) {
                break;
            }
        }
        return count;
    }

    /**
     * A fire time is expired strictly before {@code now - grace}, so the first fresh cron occurrence
     * is the first one at or after that horizon. Occurrences sit on whole seconds while the horizon
     * carries the node clock's milliseconds, so the horizon is rounded up to the next second first:
     * {@code next()} is exclusive, one second earlier it is inclusive.
     */
    private static LocalDateTime firstNotExpiredCron(JobDefinition job, LocalDateTime horizon) {
        LocalDateTime horizonSecond = horizon.getNano() == 0
                ? horizon
                : horizon.truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
        return ScheduleCalculator.parseCron(job.getCronExpr()).next(horizonSecond.minusSeconds(1));
    }

    /** {@code now - grace}: fire times before it are expired, at or after it they are fresh. */
    private static LocalDateTime graceHorizon(JobDefinition job, LocalDateTime now) {
        int grace = job.getMisfireGraceSeconds() == null ? 0 : job.getMisfireGraceSeconds();
        return now.minusSeconds(grace);
    }

    /**
     * The number of fixed-rate steps from {@code from} until the fire time is at or after
     * {@code horizon}: {@code ceil((horizon - from) / interval)}, never negative. Seconds arithmetic
     * with a remainder check instead of millis/nanos, so a horizon that does not land on a step
     * boundary cannot be truncated into the wrong step.
     */
    private static int stepsUntilNotExpired(JobDefinition job, LocalDateTime from, LocalDateTime horizon) {
        Duration distance = Duration.between(from, horizon);
        if (distance.isNegative() || distance.isZero()) {
            return 0;
        }
        long seconds = distance.toSeconds();
        long interval = intervalSeconds(job);
        boolean exact = distance.getNano() == 0 && seconds % interval == 0;
        long steps = seconds / interval;
        return (int) Math.max(0L, exact ? steps : steps + 1);
    }

    private static int intervalSeconds(JobDefinition job) {
        Integer interval = job.getIntervalSeconds();
        return interval == null || interval < 1 ? 1 : interval;
    }
}
