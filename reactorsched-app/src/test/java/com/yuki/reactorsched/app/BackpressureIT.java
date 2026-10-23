package com.yuki.reactorsched.app;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Entry-point backpressure (PRD 8.5): with a due-backlog reject threshold of 1, the third
 * unabsorbed instance pushes the submission and the manual trigger to 429, and every rejection
 * leaves a {@code BACKPRESSURE_REJECTED} decision row. No worker runs in this test, so the
 * submitted instances stay due and keep the backlog above the threshold.
 */
@org.springframework.context.annotation.Import(com.yuki.reactorsched.app.task.TestHandlersConfiguration.class)
class BackpressureIT extends AbstractIntegrationTest {

    private static final String ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "admin-test";

    @DynamicPropertySource
    static void tinyThresholds(DynamicPropertyRegistry registry) {
        registry.add("reactorsched.backpressure.backlog-reject-threshold", () -> "1");
        registry.add("reactorsched.backpressure.queue-reject-threshold", () -> "0");
    }

    @Test
    void submissionAnswers429AndLeavesADecisionOnceTheBacklogExceedsTheThreshold() throws Exception {
        submit("scenario", "{}");
        submit("scenario", "{}");
        // Two unabsorbed due instances: the next admission is refused.
        mockMvc.perform(post("/api/v1/tasks").contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"scenario\",\"payload\":\"{}\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.code").value("BACKPRESSURE_REJECTED"));

        Long decisions = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM scheduler_decision WHERE reason_code = 'BACKPRESSURE_REJECTED'",
                Long.class);
        assertThat(decisions).isGreaterThanOrEqualTo(1L);
    }

    @Test
    void manualTriggerAnswers429UnderTheSameThreshold() throws Exception {
        submit("scenario", "{}");
        submit("scenario", "{}");

        String body = """
                {"jobKey":"backpressure-demo","name":"backpressure demo","taskType":"scenario",
                 "scheduleType":"ONE_TIME","runAt":"2027-01-01T00:00:00Z"}
                """;
        String response = mockMvc.perform(post("/api/v1/jobs").param("operator", "alice")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long jobId = new com.fasterxml.jackson.databind.ObjectMapper().readTree(response).get("id").asLong();

        mockMvc.perform(post("/api/v1/jobs/" + jobId + "/trigger")
                        .with(httpBasic(ADMIN, ADMIN_PASSWORD))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"bob\",\"reason\":\"backpressure\",\"clientKey\":\"bp-1\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.code").value("BACKPRESSURE_REJECTED"));
    }
}
