package com.yuki.reactorsched.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.core.domain.JobStatus;
import com.yuki.reactorsched.core.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Lifecycle of a job definition over the real HTTP API and a real MySQL database (PRD 5.2, 13.2).
 *
 * <p>These are the S1 acceptance tests: a definition can be created, edited, paused, resumed,
 * triggered and soft deleted; instances keep their own snapshot; every administrative write leaves an
 * audit row; and a botched request is rejected with the field that caused it.
 */
class JobLifecycleIT extends AbstractIntegrationTest {

    private static final String ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "admin-test";

    @BeforeEach
    void clearJobTables() {
        jdbcTemplate.execute("DELETE FROM operation_audit");
        jdbcTemplate.execute("DELETE FROM job_definition");
    }

    @Test
    void createsADefinitionAndComputesTheNextFireTime() throws Exception {
        String body = """
                {"jobKey":"nightly-report","name":"Nightly report","taskType":"echo",
                 "scheduleType":"CRON","cronExpr":"0 0 2 * * *","shardTotal":3,
                 "payloadTemplate":{"report":"daily"}}
                """;
        mockMvc.perform(post("/api/v1/jobs").param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.jobKey").value("nightly-report"))
                .andExpect(jsonPath("$.status").value("ENABLED"))
                .andExpect(jsonPath("$.shardTotal").value(3))
                .andExpect(jsonPath("$.cronExpr").value("0 0 2 * * *"))
                .andExpect(jsonPath("$.payloadTemplate.report").value("daily"))
                .andExpect(jsonPath("$.nextFireAt").isNotEmpty());

        Long jobs = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM job_definition", Long.class);
        assertThat(jobs).isEqualTo(1L);
    }

    @Test
    void rejectsAJobWithoutCredentials() throws Exception {
        mockMvc.perform(post("/api/v1/jobs").param("operator", "alice")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"jobKey":"x","name":"x","taskType":"echo","scheduleType":"CRON",
                                 "cronExpr":"0 0 2 * * *"}
                                """))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void reportsTheFieldOfARejectedRequest() throws Exception {
        mockMvc.perform(post("/api/v1/jobs").param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"jobKey":"","name":"bad","taskType":"echo","scheduleType":"FIXED_RATE",
                                 "intervalSeconds":0,"shardTotal":0}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.details[?(@.field=='jobKey')]").exists())
                .andExpect(jsonPath("$.details[?(@.field=='intervalSeconds')]").exists());
    }

    @Test
    void rejectsACronJobWithoutACronExpression() throws Exception {
        mockMvc.perform(post("/api/v1/jobs").param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"jobKey":"no-cron","name":"no cron","taskType":"echo","scheduleType":"CRON"}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    @Test
    void pausesAndResumesWithoutCompensatingMissedFireTimes() throws Exception {
        long jobId = createFixedRateJob("pause-me", 60);
        String paused = mockMvc.perform(post("/api/v1/jobs/" + jobId + "/pause")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"maintenance\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PAUSED"))
                .andExpect(jsonPath("$.nextFireAt").isEmpty())
                .andReturn().getResponse().getContentAsString();
        assertThat(paused).contains("\"status\":\"PAUSED\"");

        mockMvc.perform(post("/api/v1/jobs/" + jobId + "/enable")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"maintenance over\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ENABLED"))
                .andExpect(jsonPath("$.nextFireAt").isNotEmpty());

        // Enabling twice is a state machine violation, not a silent no-op.
        mockMvc.perform(post("/api/v1/jobs/" + jobId + "/enable")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"again\"}"))
                .andExpect(status().isConflict());
    }

    @Test
    void editingTheScheduleMovesTheNextFireTime() throws Exception {
        long jobId = createFixedRateJob("edit-me", 3600);
        String before = jobView(jobId).get("nextFireAt").asText();

        mockMvc.perform(put("/api/v1/jobs/" + jobId).param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"jobKey":"edit-me","name":"Edit me (renamed)","taskType":"echo",
                                 "scheduleType":"CRON","cronExpr":"0 0 3 * * *"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("Edit me (renamed)"))
                .andExpect(jsonPath("$.scheduleType").value("CRON"))
                .andExpect(jsonPath("$.nextFireAt").isNotEmpty());

        JsonNode after = jobView(jobId);
        assertThat(after.get("nextFireAt").asText()).isNotEqualTo(before);
        assertThat(after.get("scheduleType").asText()).isEqualTo("CRON");
    }

    @Test
    void softDeleteKeepsTheDefinitionReadableAndTheKeyTaken() throws Exception {
        long jobId = createFixedRateJob("delete-me", 60);
        mockMvc.perform(delete("/api/v1/jobs/" + jobId)
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"no longer needed\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DELETED"))
                .andExpect(jsonPath("$.deletedAt").isNotEmpty());

        mockMvc.perform(get("/api/v1/jobs/" + jobId)).andExpect(status().isOk());

        // The key stays taken: history must never point at a different definition with the same name.
        mockMvc.perform(post("/api/v1/jobs").param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"jobKey":"delete-me","name":"again","taskType":"echo",
                                 "scheduleType":"FIXED_RATE","intervalSeconds":60}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("INVALID_JOB_STATE"));
    }

    @Test
    void everyAdministrativeWriteLeavesAnAuditRow() throws Exception {
        long jobId = createFixedRateJob("audited", 60);
        mockMvc.perform(post("/api/v1/jobs/" + jobId + "/pause")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"bob\",\"reason\":\"pause for audit\"}"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/jobs/" + jobId + "/trigger")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"bob\",\"reason\":\"manual run\",\"clientKey\":\"k1\"}"))
                .andExpect(status().isCreated());

        Long created = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM operation_audit WHERE target_type='JOB' AND target_id=? AND action='CREATE'",
                Long.class, String.valueOf(jobId));
        Long paused = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM operation_audit WHERE target_id=? AND action='PAUSE' AND operator='bob' AND reason='pause for audit'",
                Long.class, String.valueOf(jobId));
        Long triggered = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM operation_audit WHERE target_id=? AND action='TRIGGER'",
                Long.class, String.valueOf(jobId));
        assertThat(created).isEqualTo(1L);
        assertThat(paused).isEqualTo(1L);
        assertThat(triggered).isEqualTo(1L);

        // The audit row carries the before/after snapshots, not just the action name.
        String afterJson = jdbcTemplate.queryForObject(
                "SELECT after_json FROM operation_audit WHERE target_id=? AND action='PAUSE'",
                String.class, String.valueOf(jobId));
        // after_json is a MySQL JSON column: the server re-renders what it stores (key order,
        // spacing, number format), so the snapshot is verified by parsing it rather than by
        // matching a substring that only a text column would keep verbatim.
        assertThat(objectMapper.readTree(afterJson).get("status").asText())
                .as("the audit snapshot records the paused state")
                .isEqualTo("PAUSED");
    }

    @Test
    void manualTriggerIsIdempotentPerClientKey() throws Exception {
        long jobId = createFixedRateJob("trigger-me", 3600);
        String body = "{\"operator\":\"alice\",\"reason\":\"run now\",\"clientKey\":\"click-1\"}";
        String first = mockMvc.perform(post("/api/v1/jobs/" + jobId + "/trigger")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.created").value(true))
                .andExpect(jsonPath("$.instance.status").value("PENDING"))
                .andExpect(jsonPath("$.instance.jobId").value(jobId))
                .andReturn().getResponse().getContentAsString();
        long firstId = new ObjectMapper().readTree(first).get("instance").get("id").asLong();

        mockMvc.perform(post("/api/v1/jobs/" + jobId + "/trigger")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.created").value(false))
                .andExpect(jsonPath("$.instance.id").value(firstId));

        Long instances = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM task WHERE job_id = ?", Long.class, jobId);
        assertThat(instances).isEqualTo(1L);
    }

    @Test
    void instanceKeepsItsPayloadSnapshotWhenTheDefinitionChanges() throws Exception {
        long jobId = createFixedRateJobWithPayload("snapshot", 3600, "{\"mode\":\"old\"}");
        mockMvc.perform(post("/api/v1/jobs/" + jobId + "/trigger")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"run\",\"clientKey\":\"s1\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.instance.payload.mode").value("old"))
                .andExpect(jsonPath("$.instance.payload._shardIndex").value(0))
                .andExpect(jsonPath("$.instance.payload._shardTotal").value(1));

        mockMvc.perform(put("/api/v1/jobs/" + jobId).param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"jobKey":"snapshot","name":"Snapshot","taskType":"echo",
                                 "scheduleType":"FIXED_RATE","intervalSeconds":3600,
                                 "payloadTemplate":{"mode":"new"}}
                                """))
                .andExpect(status().isOk());

        String stored = jdbcTemplate.queryForObject(
                "SELECT payload FROM task WHERE job_id = ?", String.class, jobId);
        assertThat(stored).contains("\"mode\"").contains("old").doesNotContain("new");
    }

    @Test
    void unknownJobIsNotFound() throws Exception {
        mockMvc.perform(get("/api/v1/jobs/999999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("JOB_NOT_FOUND"));
    }

    @Test
    void listsDefinitionsWithStatusFilter() throws Exception {
        createFixedRateJob("list-a", 60);
        long pausedId = createFixedRateJob("list-b", 60);
        mockMvc.perform(post("/api/v1/jobs/" + pausedId + "/pause")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"filter test\"}"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/jobs").param("status", "ENABLED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].jobKey").value("list-a"));
        mockMvc.perform(get("/api/v1/jobs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(2));
    }

    private long createFixedRateJob(String jobKey, int intervalSeconds) throws Exception {
        return createFixedRateJobWithPayload(jobKey, intervalSeconds, "{}");
    }

    private long createFixedRateJobWithPayload(String jobKey, int intervalSeconds, String template) throws Exception {
        String body = """
                {"jobKey":"%s","name":"%s","taskType":"echo","scheduleType":"FIXED_RATE",
                 "intervalSeconds":%d,"payloadTemplate":%s}
                """.formatted(jobKey, jobKey, intervalSeconds, template);
        String response = mockMvc.perform(post("/api/v1/jobs").param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return new ObjectMapper().readTree(response).get("id").asLong();
    }

    private JsonNode jobView(long jobId) throws Exception {
        String response = mockMvc.perform(get("/api/v1/jobs/" + jobId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return new ObjectMapper().readTree(response);
    }


}
