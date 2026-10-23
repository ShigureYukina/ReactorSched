package com.yuki.reactorsched.core.schedule;

import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.ScheduleType;
import com.yuki.reactorsched.core.domain.MisfirePolicy;
import org.springframework.scheduling.support.CronExpression;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Works out the next planned fire time of a definition (PRD 6.1, 6.4).
 *
 * <p>Everything here is a pure function of a {@link JobDefinition} and a "now": the scheduler decides
 * what to do with a missed fire time, this class only says what the plan says.
 *
 * <p>Cron expressions are interpreted by Spring's {@link CronExpression}, never by a hand-written
 * parser (AGENTS.md 5). Six fields are required, so the smallest unit of a schedule is one second.
 */
public final class ScheduleCalculator {

    private ScheduleCalculator() {
    }

    /**
     * The first planned fire time at or after {@code from}. Returns {@code null} when the schedule
     * has no such point (a one-off that already fired, or a cron with no further occurrence).
     */
    public static LocalDateTime firstFireAtOrAfter(JobDefinition job, LocalDateTime from) {
        if (job == null || job.getScheduleType() == null) {
            return null;
        }
        return switch (job.getScheduleType()) {
            case CRON -> nextCronOccurrence(job.getCronExpr(), from);
            case FIXED_RATE -> job.getRunAt() != null && job.getRunAt().isAfter(from)
                    ? job.getRunAt()
                    : from;
            case ONE_TIME -> job.getRunAt() != null && !job.getRunAt().isBefore(from) ? job.getRunAt() : null;
        };
    }

    /**
     * The next planned fire time strictly after the one that was just claimed.
     *
     * <p>Fixed-rate schedules advance from the planned time, not from "now", so a scheduler that runs
     * late does not push every later occurrence back (PRD 6.4).
     *
     * @return the next planned time, or {@code null} when the schedule is finished (one-off)
     */
    public static LocalDateTime nextFireAtAfter(JobDefinition job, LocalDateTime plannedFireAt) {
        if (job == null || plannedFireAt == null || job.getScheduleType() == null) {
            return null;
        }
        return switch (job.getScheduleType()) {
            case CRON -> nextCronOccurrence(job.getCronExpr(), plannedFireAt.plusSeconds(1));
            case FIXED_RATE -> {
                int seconds = job.getIntervalSeconds() == null ? 0 : job.getIntervalSeconds();
                yield seconds < 1 ? null : plannedFireAt.plusSeconds(seconds);
            }
            case ONE_TIME -> null;
        };
    }

    /**
     * True when the planned fire time is late enough to count as a misfire under the definition's
     * grace period (PRD 6.4). A fire time exactly at the grace boundary is still on time.
     */
    public static boolean isMisfire(JobDefinition job, LocalDateTime plannedFireAt, LocalDateTime now) {
        if (plannedFireAt == null || now == null) {
            return false;
        }
        int grace = job.getMisfireGraceSeconds() == null ? 0 : job.getMisfireGraceSeconds();
        return Duration.between(plannedFireAt, now).compareTo(Duration.ofSeconds(grace)) > 0;
    }

    /**
     * The fire times that a catch-up policy still has to create instances for, oldest first, capped
     * at {@code maxCatchUp}. The returned list never contains a time before {@code plannedFireAt}.
     */
    public static java.util.List<LocalDateTime> missedFireTimes(JobDefinition job, LocalDateTime plannedFireAt,
                                                                int maxCatchUp, Clock clock) {
        java.util.List<LocalDateTime> missed = new java.util.ArrayList<>();
        if (job == null || plannedFireAt == null || maxCatchUp < 1) {
            return missed;
        }
        LocalDateTime now = LocalDateTime.now(clock);
        LocalDateTime cursor = plannedFireAt;
        while (cursor != null && !cursor.isAfter(now) && missed.size() < maxCatchUp) {
            if (isMisfire(job, cursor, now)) {
                missed.add(cursor);
            }
            cursor = nextFireAtAfter(job, cursor);
        }
        return missed;
    }

    /** Parses a six-field cron expression; throws {@link IllegalArgumentException} when invalid. */
    public static CronExpression parseCron(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("cron expression is required for CRON schedules");
        }
        String trimmed = expression.trim();
        if (trimmed.split("\\s+").length != 6 && !trimmed.startsWith("@")) {
            throw new IllegalArgumentException(
                    "cron expression must have six fields (second minute hour day month weekday): " + trimmed);
        }
        try {
            return CronExpression.parse(trimmed);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid cron expression '" + trimmed + "': " + e.getMessage(), e);
        }
    }

    private static LocalDateTime nextCronOccurrence(String expression, LocalDateTime from) {
        if (expression == null || expression.isBlank() || from == null) {
            return null;
        }
        return parseCron(expression).next(from);
    }

    /** The misfire policy in effect, defaulting to {@code SKIP} when nothing was configured. */
    public static MisfirePolicy misfirePolicyOf(JobDefinition job) {
        if (job == null || job.getMisfirePolicy() == null) {
            return MisfirePolicy.SKIP;
        }
        return job.getMisfirePolicy();
    }

    /** Whether the schedule can produce more than one fire time. */
    public static boolean isRecurring(ScheduleType type) {
        return type == ScheduleType.CRON || type == ScheduleType.FIXED_RATE;
    }
}
