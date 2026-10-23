package com.yuki.reactorsched.app;

import com.yuki.reactorsched.core.domain.MisfirePolicy;
import com.yuki.reactorsched.core.mapper.JobDefinitionMapper;
import com.yuki.reactorsched.core.service.InstanceFactory;
import com.yuki.reactorsched.core.service.JobService;
import com.yuki.reactorsched.scheduler.JobSchedulerService;
import com.yuki.reactorsched.scheduler.ScheduleScanResult;
import com.yuki.reactorsched.scheduler.SchedulerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two scheduler nodes competing for the same fire times (PRD 6.2): every fire time is fired exactly
 * once, no matter how the two scans interleave, and losing a race costs nothing but a counter.
 *
 * <p>Both nodes are released by the same latch and keep scanning until the due set is empty. The
 * assertion is the invariant, not the interleaving: a duplicate instance would violate the unique
 * identity key, and a fire time fired twice would show up as a second task row for one job.
 */
@SpringBootTest
@AutoConfigureMockMvc
class MultiSchedulerTriggerIT extends AbstractIntegrationTest {

    private static final LocalDateTime SCAN_NOW = LocalDateTime.of(2026, 10, 22, 12, 30, 0);
    private static final int JOB_COUNT = 20;
    private static final int SHARD_TOTAL = 2;

    @Autowired
    private JobService jobService;

    @Autowired
    private JobDefinitionMapper jobDefinitionMapper;

    @Autowired
    private InstanceFactory instanceFactory;

    @Test
    void twoSchedulersFireEachFireTimeExactlyOnce() throws Exception {
        LocalDateTime planned = SCAN_NOW.minusSeconds(2);
        List<Long> jobIds = new ArrayList<>();
        for (int i = 0; i < JOB_COUNT; i++) {
            jobIds.add(insertJob("race-" + i, planned));
        }

        int fireTimesFired;
        int lostRaces;
        int instancesPlanned;
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            CountDownLatch startGate = new CountDownLatch(1);
            JobSchedulerService nodeA = scheduler();
            JobSchedulerService nodeB = scheduler();
            Future<ScheduleScanResult> futureA = pool.submit(() -> scanUntilDrained(nodeA, startGate));
            Future<ScheduleScanResult> futureB = pool.submit(() -> scanUntilDrained(nodeB, startGate));
            startGate.countDown();
            ScheduleScanResult resultA = futureA.get(60, TimeUnit.SECONDS);
            ScheduleScanResult resultB = futureB.get(60, TimeUnit.SECONDS);
            fireTimesFired = resultA.fireTimesFired() + resultB.fireTimesFired();
            lostRaces = resultA.lostRaces() + resultB.lostRaces();
            instancesPlanned = resultA.instancesPlanned() + resultB.instancesPlanned();
        }

        assertThat(fireTimesFired).as("each fire time is fired exactly once").isEqualTo(JOB_COUNT);
        assertThat(instancesPlanned).isEqualTo(JOB_COUNT * SHARD_TOTAL);
        // Both nodes read the same due set, so the node that lost a fire time saw it as due and
        // could not advance it: the optimistic claim is what keeps the instances unique. The two
        // threads are released simultaneously and walk the same ordered list, so at least one
        // collision is certain unless one node never overlapped the other at all.
        assertThat(lostRaces).as("the race is real: the loser saw due fire times it could not claim")
                .isGreaterThanOrEqualTo(1);
        for (Long jobId : jobIds) {
            assertThat(scheduledAtOf(jobId)).as("job %d has one instance per shard, never more", jobId)
                    .containsExactly(planned, planned);
            Map<String, Object> row = jobRow(jobId);
            assertThat(row.get("next_fire_at")).isEqualTo(planned.plusSeconds(60));
            assertThat(row.get("last_fire_at")).isEqualTo(planned);
            assertThat(decisions(jobId)).as("an on-time fire time is not a misfire").isEmpty();
        }
    }

    /** Keeps scanning until the due set is empty; aggregates the rounds into one result. */
    private ScheduleScanResult scanUntilDrained(JobSchedulerService service, CountDownLatch startGate) {
        try {
            startGate.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        ScheduleScanResult total = ScheduleScanResult.EMPTY;
        ScheduleScanResult round;
        do {
            round = service.scanOnce();
            total = add(total, round);
        } while (round.jobsScanned() > 0);
        return total;
    }

    private static ScheduleScanResult add(ScheduleScanResult a, ScheduleScanResult b) {
        return new ScheduleScanResult(a.jobsScanned() + b.jobsScanned(),
                a.fireTimesFired() + b.fireTimesFired(),
                a.instancesPlanned() + b.instancesPlanned(),
                a.misfiresSkipped() + b.misfiresSkipped(),
                a.misfiresFiredLate() + b.misfiresFiredLate(),
                a.catchUpsLimited() + b.catchUpsLimited(),
                a.lostRaces() + b.lostRaces(),
                a.triggerFailures() + b.triggerFailures(),
                a.failedJobs() + b.failedJobs());
    }

    private JobSchedulerService scheduler() {
        SchedulerProperties properties = new SchedulerProperties();
        Clock fixed = Clock.fixed(SCAN_NOW.toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
        return new JobSchedulerService(jobService, jobDefinitionMapper, instanceFactory,
                schedulerDecisionRecorder, properties, fixed);
    }

    private long insertJob(String key, LocalDateTime nextFireAt) {
        jdbcTemplate.update("""
                        INSERT INTO job_definition (job_key, name, task_type, payload_template,
                            schedule_type, interval_seconds, misfire_policy, misfire_grace_seconds,
                            shard_total, status, next_fire_at, created_by, updated_by,
                            created_at, updated_at)
                        VALUES (?, ?, 'echo', '{"k":"v"}', 'FIXED_RATE', 60, ?, 60, ?, 'ENABLED', ?,
                            'it', 'it', ?, ?)
                        """,
                key, key, MisfirePolicy.SKIP.name(), SHARD_TOTAL, nextFireAt, SCAN_NOW, SCAN_NOW);
        Long id = jdbcTemplate.queryForObject(
                "SELECT id FROM job_definition WHERE job_key = ?", Long.class, key);
        assertThat(id).isNotNull();
        return id;
    }

    private List<LocalDateTime> scheduledAtOf(long jobId) {
        return jdbcTemplate.queryForList(
                "SELECT scheduled_at FROM task WHERE job_id = ? ORDER BY scheduled_at, shard_index",
                LocalDateTime.class, jobId);
    }

    private Map<String, Object> jobRow(long jobId) {
        return jdbcTemplate.queryForMap(
                "SELECT status, next_fire_at, last_fire_at FROM job_definition WHERE id = ?", jobId);
    }

    private List<Map<String, Object>> decisions(long jobId) {
        return jdbcTemplate.queryForList(
                "SELECT reason_code, observed_details FROM scheduler_decision WHERE job_id = ? ORDER BY id",
                jobId);
    }
}
