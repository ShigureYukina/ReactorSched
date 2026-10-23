package com.yuki.reactorsched.scheduler;

import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.ReasonCodes;
import com.yuki.reactorsched.core.mapper.JobDefinitionMapper;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import com.yuki.reactorsched.core.service.InstanceFactory;
import com.yuki.reactorsched.core.service.JobService;
import com.yuki.reactorsched.core.decision.SchedulerDecisionRecorder;
import com.yuki.reactorsched.core.schedule.ScheduleCalculator;
import com.yuki.reactorsched.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * One pass of the scheduler: turn the definitions whose fire time has arrived into instances
 * (PRD 6.2, 6.4).
 *
 * <p>The scan is stateless and safe to run on several nodes at the same time. Every fire time is
 * claimed with a conditional update whose {@code WHERE} clause carries the observed
 * {@code next_fire_at}, so of all the nodes that read the same due definition exactly one can
 * advance it; the rest read 0 rows and move on. No distributed lock is involved (PRD 6.2).
 *
 * <p>The fire time is advanced <em>before</em> the instances are created. A scheduler that dies
 * between the two loses the fire time instead of firing it twice, and a creation failure is recorded
 * as {@code JOB_TRIGGER_FAILED} without rolling the advance back - the definition continues with its
 * next fire time and the lost one stays explainable.
 *
 * <p>Each job is processed on its own: a definition that cannot be processed (bad schedule, database
 * hiccup) is logged and counted, and the scan continues with the rest.
 */
@Profile("!executor")
@Service
public class JobSchedulerService {

    private static final Logger log = LoggerFactory.getLogger(JobSchedulerService.class);

    private final JobService jobService;
    private final JobDefinitionMapper jobDefinitionMapper;
    private final InstanceFactory instanceFactory;
    private final SchedulerDecisionRecorder decisionRecorder;
    private final SchedulerProperties properties;
    private final Clock clock;

    /** Meters of PRD 8.6; attached after construction, absent in unit tests. */
    private volatile ReactorMetrics metrics;

    public JobSchedulerService(JobService jobService,
                               JobDefinitionMapper jobDefinitionMapper,
                               InstanceFactory instanceFactory,
                               SchedulerDecisionRecorder decisionRecorder,
                               SchedulerProperties properties,
                               Clock clock) {
        this.jobService = jobService;
        this.jobDefinitionMapper = jobDefinitionMapper;
        this.instanceFactory = instanceFactory;
        this.decisionRecorder = decisionRecorder;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Attaches the meters (PRD 8.6). Separate from the constructor so tests that build the service
     * by hand stay unchanged; without them the scheduler behaves identically.
     */
    public void attachMetrics(ReactorMetrics metrics) {
        this.metrics = metrics;
    }

    /** One scan pass; the entry point of the scheduled loop and of the tests. */
    public ScheduleScanResult scanOnce() {
        LocalDateTime now = LocalDateTime.now(clock);
        List<JobDefinition> due = jobService.dueJobs(now, properties.effectiveBatchSize());
        Counters counters = new Counters();
        for (JobDefinition job : due) {
            try {
                processJob(job, now, counters);
            } catch (RuntimeException e) {
                counters.failedJobs++;
                log.warn("job {} could not be processed this round: {}", job.getId(), e.toString());
            }
        }
        ScheduleScanResult result = counters.toResult(due.size());
        if (result.fireTimesFired() + result.misfireDecisions() + result.triggerFailures() > 0) {
            log.info("scheduler scan at {}: {} due definition(s), {} fire time(s) fired ({} instance(s)), "
                            + "{} misfire(s) skipped, {} fired late, {} catch-up(s) limited, {} lost race(s), "
                            + "{} trigger failure(s), {} failed definition(s)",
                    now, result.jobsScanned(), result.fireTimesFired(), result.instancesPlanned(),
                    result.misfiresSkipped(), result.misfiresFiredLate(), result.catchUpsLimited(),
                    result.lostRaces(), result.triggerFailures(), result.failedJobs());
        }
        return result;
    }

    private void processJob(JobDefinition job, LocalDateTime now, Counters counters) {
        LocalDateTime planned = job.getNextFireAt();
        if (planned == null) {
            // dueJobs only returns rows with a fire time; a null here means the row moved on.
            return;
        }
        if (!ScheduleCalculator.isMisfire(job, planned, now)) {
            fireOnTime(job, planned, now, counters);
            return;
        }
        switch (ScheduleCalculator.misfirePolicyOf(job)) {
            case SKIP -> skipMissedFireTimes(job, planned, now, counters);
            case FIRE_ONCE -> fireOnceLate(job, planned, now, counters);
            case CATCH_UP -> catchUpMissedFireTimes(job, planned, now, counters);
        }
    }

    /** The ordinary case: the fire time arrived inside its grace period. */
    private void fireOnTime(JobDefinition job, LocalDateTime planned, LocalDateTime now, Counters counters) {
        LocalDateTime next = InstanceFactory.nextFire(job, planned).nextFireAt();
        if (next == null) {
            // The schedule is finished: a one-off, or a cron that has no further occurrence. The
            // planned time is fired and the definition is parked so the scanner stops seeing it.
            if (jobDefinitionMapper.claimFinalFireTime(job.getId(), planned, planned, now) == 1) {
                createInstances(job, planned, now, counters);
            } else {
                counters.lostRaces++;
            }
            return;
        }
        if (jobDefinitionMapper.claimFireTime(job.getId(), planned, next, planned, now) == 1) {
            createInstances(job, planned, now, counters);
        } else {
            counters.lostRaces++;
        }
    }

    /** {@code SKIP}: drop every missed fire time and resume at the first fresh one (PRD 6.4). */
    private void skipMissedFireTimes(JobDefinition job, LocalDateTime planned, LocalDateTime now,
                                     Counters counters) {
        int missed = MissedFireTimes.countMissed(job, planned, now);
        LocalDateTime target = MissedFireTimes.firstNotExpired(job, planned, now);
        boolean claimed = target == null
                ? jobDefinitionMapper.retireDefinition(job.getId(), planned, now) == 1
                : jobDefinitionMapper.skipToNextFireTime(job.getId(), planned, target, now) == 1;
        if (!claimed) {
            counters.lostRaces++;
            return;
        }
        counters.misfiresSkipped++;
        decisionRecorder.recordMisfire(job.getId(), planned, policyName(job), ReasonCodes.MISFIRE_SKIPPED,
                0, missed, now);
    }

    /** {@code FIRE_ONCE}: one make-up instance for the most recent missed fire time (PRD 6.4). */
    private void fireOnceLate(JobDefinition job, LocalDateTime planned, LocalDateTime now, Counters counters) {
        int missed = MissedFireTimes.countMissed(job, planned, now);
        LocalDateTime target = MissedFireTimes.firstNotExpired(job, planned, now);
        if (target == null) {
            // A one-off whose time passed: its single fire time gets the make-up instance.
            if (jobDefinitionMapper.claimFinalFireTime(job.getId(), planned, planned, now) == 1) {
                createInstances(job, planned, now, counters);
                counters.misfiresFiredLate++;
                decisionRecorder.recordMisfire(job.getId(), planned, policyName(job),
                        ReasonCodes.MISFIRE_FIRED_LATE, shardsOf(job), missed, now);
            } else {
                counters.lostRaces++;
            }
            return;
        }
        // The instance carries the original planned time of the make-up firing; created_at keeps the
        // moment it was actually written (PRD 6.4).
        LocalDateTime makeUpFor = MissedFireTimes.mostRecentMissed(job, planned, now);
        if (jobDefinitionMapper.claimFireTime(job.getId(), planned, target, makeUpFor, now) == 1) {
            createInstances(job, makeUpFor, now, counters);
            counters.misfiresFiredLate++;
            decisionRecorder.recordMisfire(job.getId(), makeUpFor, policyName(job),
                    ReasonCodes.MISFIRE_FIRED_LATE, shardsOf(job), missed, now);
        } else {
            counters.lostRaces++;
        }
    }

    /**
     * {@code CATCH_UP}: one instance set per missed fire time, up to the catch-up limit. Each fire
     * time is claimed and fired on its own, so a crash halfway through resumes at the next missed
     * time instead of losing the rest. Whatever is beyond the limit is skipped and reported
     * (PRD 6.4).
     */
    private void catchUpMissedFireTimes(JobDefinition job, LocalDateTime planned, LocalDateTime now,
                                        Counters counters) {
        // A fixed clock wrapping the scan's "now" keeps the enumeration and the claims on one notion
        // of time; missedFireTimes would otherwise read its own now().
        Clock scanNow = Clock.fixed(now.toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
        List<LocalDateTime> missed = ScheduleCalculator.missedFireTimes(
                job, planned, properties.effectiveMaxCatchUp(), scanNow);
        LocalDateTime cursor = planned;
        for (LocalDateTime fireTime : missed) {
            LocalDateTime next = InstanceFactory.nextFire(job, fireTime).nextFireAt();
            boolean claimed = next == null
                    ? jobDefinitionMapper.claimFinalFireTime(job.getId(), cursor, fireTime, now) == 1
                    : jobDefinitionMapper.claimFireTime(job.getId(), cursor, next, fireTime, now) == 1;
            if (!claimed) {
                // Another scheduler owns the remaining catch-up of this definition.
                counters.lostRaces++;
                return;
            }
            createInstances(job, fireTime, now, counters);
            counters.misfiresFiredLate++;
            decisionRecorder.recordMisfire(job.getId(), fireTime, policyName(job),
                    ReasonCodes.MISFIRE_FIRED_LATE, shardsOf(job), missed.size(), now);
            if (next == null) {
                return; // a one-off is finished
            }
            cursor = next;
        }
        if (missed.size() < properties.effectiveMaxCatchUp() || !ScheduleCalculator.isMisfire(job, cursor, now)) {
            return; // everything that was missed has been fired
        }
        // More missed fire times than the limit allows: the rest is skipped, not left for the next
        // scan to fire one by one.
        LocalDateTime target = MissedFireTimes.firstNotExpired(job, cursor, now);
        boolean claimed = target == null
                ? jobDefinitionMapper.retireDefinition(job.getId(), cursor, now) == 1
                : jobDefinitionMapper.skipToNextFireTime(job.getId(), cursor, target, now) == 1;
        if (!claimed) {
            counters.lostRaces++;
            return;
        }
        counters.catchUpsLimited++;
        decisionRecorder.recordMisfire(job.getId(), cursor, policyName(job),
                ReasonCodes.MISFIRE_CATCH_UP_LIMIT, 0, missed.size(), now);
    }

    /**
     * Creates the instances of one claimed fire time. The fire time was already advanced, so a
     * failure here is a lost fire time, not a retry: it is recorded and the scan moves on
     * (PRD 6.2).
     */
    private void createInstances(JobDefinition job, LocalDateTime planned, LocalDateTime now,
                                 Counters counters) {
        try {
            List<Long> ids = instanceFactory.createInstances(job, planned);
            counters.instancesPlanned += ids.size();
            counters.fireTimesFired++;
            if (metrics != null) {
                metrics.jobTriggered(policyName(job), "fired");
            }
        } catch (RuntimeException e) {
            counters.triggerFailures++;
            if (metrics != null) {
                metrics.jobTriggered(policyName(job), "failed");
            }
            decisionRecorder.recordJobTriggerFailure(job.getId(), planned, Messages.describe(e), now);
            log.warn("job {} fire time {} was claimed but instance creation failed: {}",
                    job.getId(), planned, e.toString());
        }
    }

    private static String policyName(JobDefinition job) {
        return ScheduleCalculator.misfirePolicyOf(job).name();
    }

    private static int shardsOf(JobDefinition job) {
        Integer shardTotal = job.getShardTotal();
        return shardTotal == null || shardTotal < 1 ? 1 : shardTotal;
    }

    /** Mutable per-scan counters; the scan result is built once at the end. */
    private static final class Counters {
        private int fireTimesFired;
        private int instancesPlanned;
        private int misfiresSkipped;
        private int misfiresFiredLate;
        private int catchUpsLimited;
        private int lostRaces;
        private int triggerFailures;
        private int failedJobs;

        private ScheduleScanResult toResult(int jobsScanned) {
            return new ScheduleScanResult(jobsScanned, fireTimesFired, instancesPlanned, misfiresSkipped,
                    misfiresFiredLate, catchUpsLimited, lostRaces, triggerFailures, failedJobs);
        }
    }
}
