package com.yuki.reactorsched.app;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Group routing of the pull contract (PRD 7.2): an executor only ever sees instances of its own
 * group, and the constraint sits in the claim's {@code WHERE} clause, not in an {@code if} above
 * the database.
 */
class GroupAllocationIT extends AbstractExecutorIT {

    private static final String ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "admin-test";

    @Test
    void handsEachGroupOnlyItsOwnInstances() throws Exception {
        long alphaJob = createJob("alpha-job", "alpha");
        long betaJob = createJob("beta-job", "beta");
        long alphaTask = trigger(alphaJob);
        long betaTask = trigger(betaJob);

        long alphaExecutor = register("grouped", "alpha-1", "alpha");
        long betaExecutor = register("grouped", "beta-1", "beta");

        mockMvc.perform(post("/api/v1/executors/{id}/tasks/claim", alphaExecutor)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"batchSize": 10}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].taskId").value(alphaTask))
                .andExpect(jsonPath("$[0].attempt").value(1))
                .andExpect(jsonPath("$[0].fencingToken").isNumber())
                .andExpect(jsonPath("$[0].leaseUntil").isNotEmpty());

        mockMvc.perform(post("/api/v1/executors/{id}/tasks/claim", betaExecutor)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"batchSize": 10}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].taskId").value(betaTask));

        // The instances are owned by their group's executor now, and nothing is left for either.
        assertThat(leaseOwnerOf(alphaTask)).isEqualTo("alpha-1");
        assertThat(leaseOwnerOf(betaTask)).isEqualTo("beta-1");
        assertThat(claim(betaExecutor)).isEmpty();
        assertThat(claim(alphaExecutor)).isEmpty();
    }

    @Test
    void leavesAnExecutorOfAnotherGroupWithoutWork() throws Exception {
        long job = createJob("lonely-job", "gamma");
        trigger(job);

        long executor = register("grouped", "delta-1", "delta");
        assertThat(claim(executor)).isEmpty();

        // Nothing was claimed, so nothing was written either.
        Long running = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM task WHERE status = 'RUNNING'", Long.class);
        assertThat(running).isZero();
    }

    // ------------------------------------------------------------------ helpers

    private long createJob(String jobKey, String group) throws Exception {
        String body = mockMvc.perform(post("/api/v1/jobs").param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"jobKey":"%s","name":"%s","taskType":"echo","scheduleType":"CRON",
                                 "cronExpr":"0 0 2 * * *","executorGroup":"%s","shardTotal":1,
                                 "payloadTemplate":{"sleepMs": 1}}
                                """.formatted(jobKey, jobKey, group)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).get("id").asLong();
    }

    /** Triggers one instance of a definition and returns the id of the instance that was created. */
    private long trigger(long jobId) throws Exception {
        String body = mockMvc.perform(post("/api/v1/jobs/{id}/trigger", jobId)
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"operator":"alice","reason":"group allocation test",
                                 "clientKey":"group-test-%d"}
                                """.formatted(jobId)))
                .andExpect(status().is2xxSuccessful())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).get("instance").get("id").asLong();
    }

    private String leaseOwnerOf(long taskId) {
        return jdbcTemplate.queryForObject(
                "SELECT lease_owner FROM task WHERE id = ?", String.class, taskId);
    }

    private long register(String appName, String instanceId, String group) throws Exception {
        String body = mockMvc.perform(post("/api/v1/executors/register")
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"appName":"%s","instanceId":"%s","group":"%s"}
                                """.formatted(appName, instanceId, group)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).get("id").asLong();
    }

    /** Claims once and returns the task ids the center handed out. */
    private List<Long> claim(long executorId) throws Exception {
        String body = mockMvc.perform(post("/api/v1/executors/{id}/tasks/claim", executorId)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"batchSize": 10}
                                """))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).findValuesAsText("taskId").stream()
                .map(Long::valueOf).toList();
    }
}
