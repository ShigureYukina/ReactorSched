package com.yuki.reactorsched.scheduler;

/**
 * Counts of one scheduler scan, used for logging and tests.
 *
 * <p>{@code lostRaces} are fire times another scheduler node claimed first - the expected outcome of
 * running several schedulers against one database, not an error (PRD 6.2).
 */
public record ScheduleScanResult(int jobsScanned,
                                 int fireTimesFired,
                                 int instancesPlanned,
                                 int misfiresSkipped,
                                 int misfiresFiredLate,
                                 int catchUpsLimited,
                                 int lostRaces,
                                 int triggerFailures,
                                 int failedJobs) {

    public static final ScheduleScanResult EMPTY =
            new ScheduleScanResult(0, 0, 0, 0, 0, 0, 0, 0, 0);

    public int misfireDecisions() {
        return misfiresSkipped + misfiresFiredLate + catchUpsLimited;
    }
}
