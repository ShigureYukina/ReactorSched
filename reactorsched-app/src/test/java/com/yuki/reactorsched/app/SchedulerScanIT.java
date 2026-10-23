package com.yuki.reactorsched.app;

import com.yuki.reactorsched.core.domain.MisfirePolicy;
import com.yuki.reactorsched.core.domain.ReasonCodes;
import com.yuki.reactorsched.core.domain.ScheduleType;
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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One scheduler scan against a real MySQL database: due definitions become instances, fire times
 * advance under competition, and every misfire policy decides and records (PRD 6.2, 6.4).
 *
 * <p>Every service under test runs on a fixed clock, so the fire time of every assertion is the one
 * the test chose instead of the one the machine happened to be at. The background scanner itself is
 * disabled for the whole test context; each test drives {@code scanOnce()} directly.
 */
@SpringBootTest
@AutoConfigureMockMvc
class SchedulerScanIT extends AbstractIntegrationTest {

    /** The scan's notion of "now"; every planned time below is relative to it. */
    private static final LocalDateTime SCAN_NOW = LocalDateTime.of(2026, 10, 22, 12, 30, 0);

    @Autowired
    private JobService jobService;

    @Autowired
    private JobDefinitionMapper jobDefinitionMapper;

    @Autowired
    private InstanceFactory instanceFactory;

    @Test
    void fixedRateFiresOnTimeAndAdvancesThePlan() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(2);
        long jobId = insertJob("on-time-fixed", ScheduleType.FIXED_RATE, null, 60,
                MisfirePolicy.SKIP, 60, 1, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.jobsScanned()).isEqualTo(1);
        assertThat(result.fireTimesFired()).isEqualTo(1);
        assertThat(result.instancesPlanned()).isEqualTo(1);
        assertThat(result.misfireDecisions()).isZero();
        assertThat(scheduledAtOf(jobId)).containsExactly(planned);
        Map<String, Object> row = jobRow(jobId);
        assertThat(row.get("status")).isEqualTo("ENABLED");
        assertThat(row.get("next_fire_at")).isEqualTo(planned.plusSeconds(60));
        assertThat(row.get("last_fire_at")).isEqualTo(planned);
        assertThat(decisions(jobId)).isEmpty();
    }

    @Test
    void oneTimeFiresOnceAndParksTheDefinition() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(2);
        long jobId = insertJob("one-time", ScheduleType.ONE_TIME, null, null,
                MisfirePolicy.SKIP, 60, 1, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.fireTimesFired()).isEqualTo(1);
        assertThat(scheduledAtOf(jobId)).containsExactly(planned);
        Map<String, Object> row = jobRow(jobId);
        // A finished plan must not stay schedulable: paused, no next fire time, the fire time kept.
        assertThat(row.get("status")).isEqualTo("PAUSED");
        assertThat(row.get("next_fire_at")).isNull();
        assertThat(row.get("last_fire_at")).isEqualTo(planned);
        // A second scan must not fire it again.
        assertThat(scheduler(20).scanOnce().jobsScanned()).isZero();
        assertThat(scheduledAtOf(jobId)).containsExactly(planned);
    }

    @Test
    void cronFiresOnTimeAndResumesAtTheNextOccurrence() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(2);
        long jobId = insertJob("on-time-cron", ScheduleType.CRON, "0 * * * * *", null,
                MisfirePolicy.SKIP, 60, 1, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.fireTimesFired()).isEqualTo(1);
        assertThat(scheduledAtOf(jobId)).containsExactly(planned);
        Map<String, Object> row = jobRow(jobId);
        assertThat(row.get("status")).isEqualTo("ENABLED");
        assertThat(row.get("next_fire_at")).isEqualTo(SCAN_NOW); // the next whole minute
        assertThat(row.get("last_fire_at")).isEqualTo(planned);
    }

    @Test
    void skipCollapsesTheMissedFireTimesWithoutFiring() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(605);
        long jobId = insertJob("misfire-skip", ScheduleType.FIXED_RATE, null, 60,
                MisfirePolicy.SKIP, 0, 1, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.misfiresSkipped()).isEqualTo(1);
        assertThat(result.fireTimesFired()).isZero();
        assertThat(scheduledAtOf(jobId)).isEmpty();
        Map<String, Object> row = jobRow(jobId);
        // 11 occurrences are past their grace period; the plan resumes at the 12th.
        assertThat(row.get("next_fire_at")).isEqualTo(SCAN_NOW.plusSeconds(55));
        assertThat(row.get("last_fire_at")).isNull();
        List<Map<String, Object>> decisions = decisions(jobId);
        assertThat(decisions).hasSize(1);
        assertThat(decisions.get(0).get("reason_code")).isEqualTo(ReasonCodes.MISFIRE_SKIPPED);
        assertThat(decisions.get(0).get("observed_details").toString())
                .contains("policy SKIP", "11 fire time(s) past their grace period");
    }

    @Test
    void fireOnceMakesUpTheMostRecentMissedFireTime() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(605);
        long jobId = insertJob("misfire-fire-once", ScheduleType.FIXED_RATE, null, 60,
                MisfirePolicy.FIRE_ONCE, 0, 1, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.misfiresFiredLate()).isEqualTo(1);
        assertThat(result.instancesPlanned()).isEqualTo(1);
        // The make-up instance carries the original planned time, not the moment it was written.
        assertThat(scheduledAtOf(jobId)).containsExactly(SCAN_NOW.minusSeconds(5));
        Map<String, Object> row = jobRow(jobId);
        assertThat(row.get("next_fire_at")).isEqualTo(SCAN_NOW.plusSeconds(55));
        assertThat(row.get("last_fire_at")).isEqualTo(SCAN_NOW.minusSeconds(5));
        List<Map<String, Object>> decisions = decisions(jobId);
        assertThat(decisions).hasSize(1);
        assertThat(decisions.get(0).get("reason_code")).isEqualTo(ReasonCodes.MISFIRE_FIRED_LATE);
        assertThat(decisions.get(0).get("observed_details").toString())
                .contains("policy FIRE_ONCE", "11 fire time(s) past their grace period");
    }

    @Test
    void catchUpFiresEveryMissedFireTimeWhenUnderTheLimit() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(605);
        long jobId = insertJob("misfire-catch-up", ScheduleType.FIXED_RATE, null, 60,
                MisfirePolicy.CATCH_UP, 0, 1, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        // 11 occurrences are past their grace period, one instance each, oldest first.
        assertThat(result.misfiresFiredLate()).isEqualTo(11);
        assertThat(result.instancesPlanned()).isEqualTo(11);
        assertThat(result.catchUpsLimited()).isZero();
        assertThat(scheduledAtOf(jobId)).containsExactly(
                planned, planned.plusSeconds(60), planned.plusSeconds(120), planned.plusSeconds(180),
                planned.plusSeconds(240), planned.plusSeconds(300), planned.plusSeconds(360),
                planned.plusSeconds(420), planned.plusSeconds(480), planned.plusSeconds(540),
                planned.plusSeconds(600));
        Map<String, Object> row = jobRow(jobId);
        assertThat(row.get("next_fire_at")).isEqualTo(SCAN_NOW.plusSeconds(55));
        assertThat(row.get("last_fire_at")).isEqualTo(planned.plusSeconds(600));
        assertThat(decisions(jobId)).hasSize(11);
    }

    @Test
    void catchUpStopsAtTheLimitAndSkipsTheRest() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(605);
        long jobId = insertJob("misfire-catch-up-limit", ScheduleType.FIXED_RATE, null, 60,
                MisfirePolicy.CATCH_UP, 0, 1, planned);

        ScheduleScanResult result = scheduler(3).scanOnce();

        assertThat(result.misfiresFiredLate()).isEqualTo(3);
        assertThat(result.instancesPlanned()).isEqualTo(3);
        assertThat(result.catchUpsLimited()).isEqualTo(1);
        assertThat(scheduledAtOf(jobId)).containsExactly(
                planned, planned.plusSeconds(60), planned.plusSeconds(120));
        Map<String, Object> row = jobRow(jobId);
        assertThat(row.get("next_fire_at")).isEqualTo(SCAN_NOW.plusSeconds(55));
        assertThat(row.get("last_fire_at")).isEqualTo(planned.plusSeconds(120));
        List<Map<String, Object>> decisionRows = decisions(jobId);
        assertThat(decisionRows).hasSize(4);
        assertThat(decisionRows.stream().filter(d -> ReasonCodes.MISFIRE_CATCH_UP_LIMIT
                .equals(d.get("reason_code")))).hasSize(1);
        assertThat(decisionRows.get(3).get("observed_details").toString())
                .contains("policy CATCH_UP", "3 fire time(s) past their grace period");
        // The next scan sees a fresh fire time, not another backlog of missed ones.
        assertThat(scheduler(3).scanOnce().jobsScanned()).isZero();
        assertThat(scheduledAtOf(jobId)).hasSize(3);
    }

    @Test
    void oneTimeUnderFireOnceMakesUpItsSingleFireTime() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(605);
        long jobId = insertJob("one-time-fire-once", ScheduleType.ONE_TIME, null, null,
                MisfirePolicy.FIRE_ONCE, 0, 1, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.misfiresFiredLate()).isEqualTo(1);
        assertThat(scheduledAtOf(jobId)).containsExactly(planned);
        Map<String, Object> row = jobRow(jobId);
        assertThat(row.get("status")).isEqualTo("PAUSED");
        assertThat(row.get("next_fire_at")).isNull();
        assertThat(row.get("last_fire_at")).isEqualTo(planned);
        assertThat(decisions(jobId).get(0).get("observed_details").toString())
                .contains("1 fire time(s) past their grace period");
    }

    @Test
    void oneTimeUnderSkipRetiresWithoutFiring() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(605);
        long jobId = insertJob("one-time-skip", ScheduleType.ONE_TIME, null, null,
                MisfirePolicy.SKIP, 0, 1, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.misfiresSkipped()).isEqualTo(1);
        assertThat(result.fireTimesFired()).isZero();
        assertThat(scheduledAtOf(jobId)).isEmpty();
        Map<String, Object> row = jobRow(jobId);
        assertThat(row.get("status")).isEqualTo("PAUSED");
        assertThat(row.get("next_fire_at")).isNull();
        assertThat(row.get("last_fire_at")).isNull();
    }

    @Test
    void shardedDefinitionCreatesOneInstancePerShard() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(2);
        long jobId = insertJob("sharded", ScheduleType.FIXED_RATE, null, 60,
                MisfirePolicy.SKIP, 60, 3, planned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.instancesPlanned()).isEqualTo(3);
        assertThat(scheduledAtOf(jobId)).containsExactly(planned, planned, planned);
        assertThat(shardIndexesOf(jobId)).containsExactly(0, 1, 2);
    }

    @Test
    void aFailedDefinitionDoesNotStopTheScan() {
        LocalDateTime planned = SCAN_NOW.minusSeconds(605);
        // A cron definition without an expression: the schedule cannot be parsed, so the misfire
        // decision cannot be taken either. The scan must still fire the healthy definition.
        long brokenId = insertJob("broken-cron", ScheduleType.CRON, null, null,
                MisfirePolicy.SKIP, 0, 1, planned);
        LocalDateTime healthyPlanned = SCAN_NOW.minusSeconds(2);
        long healthyId = insertJob("healthy-fixed", ScheduleType.FIXED_RATE, null, 60,
                MisfirePolicy.SKIP, 60, 1, healthyPlanned);

        ScheduleScanResult result = scheduler(20).scanOnce();

        assertThat(result.jobsScanned()).isEqualTo(2);
        assertThat(result.failedJobs()).isEqualTo(1);
        assertThat(result.fireTimesFired()).isEqualTo(1);
        assertThat(scheduledAtOf(brokenId)).isEmpty();
        assertThat(scheduledAtOf(healthyId)).containsExactly(healthyPlanned);
        // The broken definition keeps its fire time for the next scan, it is not lost.
        assertThat(jobRow(brokenId).get("next_fire_at")).isEqualTo(planned);
    }

    private JobSchedulerService scheduler(int maxCatchUp) {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setMaxCatchUp(maxCatchUp);
        Clock fixed = Clock.fixed(SCAN_NOW.toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
        return new JobSchedulerService(jobService, jobDefinitionMapper, instanceFactory,
                schedulerDecisionRecorder, properties, fixed);
    }

    private long insertJob(String key, ScheduleType type, String cronExpr, Integer intervalSeconds,
                           MisfirePolicy policy, int graceSeconds, int shardTotal,
                           LocalDateTime nextFireAt) {
        jdbcTemplate.update("""
                        INSERT INTO job_definition (job_key, name, task_type, payload_template,
                            schedule_type, cron_expr, interval_seconds, misfire_policy,
                            misfire_grace_seconds, shard_total, status, next_fire_at,
                            created_by, updated_by, created_at, updated_at)
                        VALUES (?, ?, 'echo', '{"k":"v"}', ?, ?, ?, ?, ?, ?, 'ENABLED', ?,
                            'it', 'it', ?, ?)
                        """,
                key, key, type.name(), cronExpr, intervalSeconds, policy.name(), graceSeconds,
                shardTotal, nextFireAt, SCAN_NOW, SCAN_NOW);
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

    private List<Integer> shardIndexesOf(long jobId) {
        return jdbcTemplate.queryForList(
                "SELECT shard_index FROM task WHERE job_id = ? ORDER BY shard_index",
                Integer.class, jobId);
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
