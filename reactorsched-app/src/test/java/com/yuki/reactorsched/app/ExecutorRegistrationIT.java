package com.yuki.reactorsched.app;

import com.yuki.reactorsched.executor.registry.service.ExecutorRegistryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Registration, heartbeat, the shared executor token and the offline sweep (PRD 7.1, 10.3).
 *
 * <p>Every call goes over real HTTP on the random port, so the token filter, the controller and the
 * registry services are all part of the assertion.
 */
class ExecutorRegistrationIT extends AbstractExecutorIT {

    @Autowired
    private ExecutorRegistryService executorRegistryService;

    @Test
    void registersAndListsTheExecutor() throws Exception {
        mockMvc.perform(post("/api/v1/executors/register")
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"appName":"report-executor","instanceId":"report-1","group":"DEFAULT",
                                 "maxConcurrency":4,"address":"10.0.0.7:9000","metadata":"{\\"version\\":\\"1.2\\"}"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").isNumber())
                .andExpect(jsonPath("$.appName").value("report-executor"))
                .andExpect(jsonPath("$.instanceId").value("report-1"))
                .andExpect(jsonPath("$.groupName").value("DEFAULT"))
                .andExpect(jsonPath("$.status").value("ONLINE"));

        // The read endpoint is open, like the rest of the query API (PRD 10.3).
        mockMvc.perform(get("/api/v1/executors"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].instanceId").value("report-1"))
                .andExpect(jsonPath("$[0].status").value("ONLINE"));
    }

    @Test
    void registersTheSameInstanceBackOntoItsRow() throws Exception {
        for (int i = 0; i < 2; i++) {
            mockMvc.perform(post("/api/v1/executors/register")
                            .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                    {"appName":"restarted","instanceId":"restarted-1"}
                                    """))
                    .andExpect(status().isOk());
        }

        Long rows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM executor_registry", Long.class);
        assertThat(rows).isEqualTo(1L);
        mockMvc.perform(get("/api/v1/executors"))
                .andExpect(jsonPath("$", hasSize(1)));
    }

    @Test
    void rejectsExecutorCallsWithoutTheSharedToken() throws Exception {
        mockMvc.perform(post("/api/v1/executors/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"appName":"no-token","instanceId":"no-token-1"}
                                """))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));

        mockMvc.perform(post("/api/v1/executors/register")
                        .header("X-ReactorSched-Token", "wrong-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"appName":"no-token","instanceId":"no-token-1"}
                                """))
                .andExpect(status().isUnauthorized());

        Long rows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM executor_registry", Long.class);
        assertThat(rows).isZero();
    }

    @Test
    void keepsTheExecutorOnlineWhileItHeartbeats() throws Exception {
        long id = register("heartbeat-happy", "heartbeat-1");

        mockMvc.perform(post("/api/v1/executors/{id}/heartbeat", id)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.status").value("ONLINE"));
    }

    @Test
    void answersAHeartbeatForAnUnknownExecutorWithNotFound() throws Exception {
        mockMvc.perform(post("/api/v1/executors/{id}/heartbeat", 999999)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EXECUTOR_NOT_FOUND"));
    }

    @Test
    void sweepsAStaleExecutorOfflineAndRefusesToHandItWork() throws Exception {
        long id = register("stale", "stale-1");
        // The heartbeat went missing a minute ago: three missed heartbeats is already stale.
        jdbcTemplate.execute("UPDATE executor_registry SET last_heartbeat_at = NOW(3) - INTERVAL 60 SECOND");

        int swept = executorRegistryService.sweepOffline();
        assertThat(swept).isEqualTo(1);

        mockMvc.perform(post("/api/v1/executors/{id}/tasks/claim", id)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"batchSize": 5}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("EXECUTOR_OFFLINE"));

        // A heartbeat proves the process is alive after all: it comes back ONLINE.
        mockMvc.perform(post("/api/v1/executors/{id}/heartbeat", id)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ONLINE"));
    }

    @Test
    void answersAClaimForAnUnknownExecutorWithNotFound() throws Exception {
        mockMvc.perform(post("/api/v1/executors/{id}/tasks/claim", 999999)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"batchSize": 5}
                                """))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EXECUTOR_NOT_FOUND"));
    }

    @Test
    void rejectsARegistrationWithoutAnAppName() throws Exception {
        mockMvc.perform(post("/api/v1/executors/register")
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"appName":"","instanceId":"x"}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    // ------------------------------------------------------------------ helpers

    private long register(String appName, String instanceId) throws Exception {
        String body = mockMvc.perform(post("/api/v1/executors/register")
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"appName":"%s","instanceId":"%s"}
                                """.formatted(appName, instanceId)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body).get("id").asLong();
    }
}
