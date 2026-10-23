package com.yuki.reactorsched.app;

import com.yuki.reactorsched.core.domain.ExecutionResult;
import com.yuki.reactorsched.core.domain.ErrorCodes;
import com.yuki.reactorsched.core.domain.ReasonCodes;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.executor.api.ErrorCategory;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Result reporting and lease renewal over the executor contract (PRD 7.3, 8.2, 9).
 *
 * <p>The interesting cases are the rejections: a stale lease version closes the attempt as
 * {@code ABANDONED} and answers 409, an attempt owned by another executor is a 403 even though the
 * fleet token is shared, and an executor that refuses an attempt after claiming it gets the instance
 * back to {@code RETRY_WAIT}.
 */
class ExecutorResultIT extends AbstractExecutorIT {

    @Test
    void reportsSuccessAndStoresTheLog() throws Exception {
        long executor = register("reporter", "reporter-1");
        long taskId = submit("echo", "{\"sleepMs\": 5}").task().id();
        Claimed claimed = claim(executor);

        mockMvc.perform(post("/api/v1/executors/{id}/executions/{executionId}/result",
                        executor, claimed.executionId())
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"leaseVersion":%d,"outcome":"SUCCEEDED",
                                 "startedAt":"2026-10-22T10:00:00","finishedAt":"2026-10-22T10:00:01",
                                 "durationMs":1000,"log":"handler finished\\nhandler finished again",
                                 "logTruncated":false}
                                """.formatted(claimed.fencingToken())))
                .andExpect(status().isNoContent());

        awaitStatus(taskId, TaskStatus.SUCCEEDED, TIMEOUT);
        List<com.yuki.reactorsched.core.dto.TaskExecutionView> attempts = executions(taskId);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).result()).isEqualTo(ExecutionResult.SUCCEEDED);
        assertThat(attempts.get(0).fencingToken()).isEqualTo(claimed.fencingToken());
        assertThat(attempts.get(0).workerId()).isEqualTo("reporter-1");

        // The log the executor reported is readable through the open log endpoint (PRD 9).
        mockMvc.perform(get("/api/v1/executions/{id}/log", claimed.executionId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.log").value("handler finished\nhandler finished again"))
                .andExpect(jsonPath("$.truncated").value(false));
    }

    @Test
    void closesTheAttemptAsAbandonedWhenTheLeaseVersionIsStale() throws Exception {
        long executor = register("reporter", "reporter-1");
        long taskId = submit("echo", "{\"sleepMs\": 5}").task().id();
        Claimed claimed = claim(executor);

        mockMvc.perform(post("/api/v1/executors/{id}/executions/{executionId}/result",
                        executor, claimed.executionId())
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"leaseVersion":%d,"outcome":"SUCCEEDED","log":"too late"}
                                """.formatted(claimed.fencingToken() + 99)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("LEASE_VERSION_STALE"));

        List<com.yuki.reactorsched.core.dto.TaskExecutionView> attempts = executions(taskId);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).result()).isEqualTo(ExecutionResult.ABANDONED);
        assertThat(attempts.get(0).errorCode()).isEqualTo(ErrorCodes.STALE_WRITER);
        assertThat(attempts.get(0).errorCategory()).isEqualTo(ErrorCategory.EXECUTOR_LOST);
        // The stale write did not touch the instance: another owner may still take it over.
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    void refusesAResultForAnAttemptOfAnotherExecutor() throws Exception {
        long owner = register("reporter", "owner-1");
        long other = register("reporter", "other-1");
        submit("echo", "{\"sleepMs\": 5}");
        Claimed claimed = claim(owner);

        mockMvc.perform(post("/api/v1/executors/{id}/executions/{executionId}/result",
                        other, claimed.executionId())
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"leaseVersion":%d,"outcome":"SUCCEEDED"}
                                """.formatted(claimed.fencingToken())))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("EXECUTOR_MISMATCH"));
    }

    @Test
    void returnsTheInstanceToRetryWaitWhenTheExecutorRejectsIt() throws Exception {
        long executor = register("reporter", "reporter-1");
        long taskId = submit("echo", "{\"sleepMs\": 5}").task().id();
        Claimed claimed = claim(executor);

        mockMvc.perform(post("/api/v1/executors/{id}/executions/{executionId}/result",
                        executor, claimed.executionId())
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"leaseVersion":%d,"outcome":"REJECTED",
                                 "message":"bounded queue is full"}
                                """.formatted(claimed.fencingToken())))
                .andExpect(status().isNoContent());

        awaitStatus(taskId, TaskStatus.RETRY_WAIT, TIMEOUT);
        assertThat(task(taskId).lastError()).contains(ReasonCodes.EXECUTOR_REJECTED);
        List<com.yuki.reactorsched.core.dto.TaskExecutionView> attempts = executions(taskId);
        assertThat(attempts.get(0).errorCode()).isEqualTo(ReasonCodes.EXECUTOR_REJECTED);
        assertThat(attempts.get(0).errorCategory()).isEqualTo(ErrorCategory.EXECUTOR_LOST);
    }

    @Test
    void renewsTheLeaseOfARunningAttempt() throws Exception {
        long executor = register("reporter", "reporter-1");
        submit("echo", "{\"sleepMs\": 5}");
        Claimed claimed = claim(executor);

        mockMvc.perform(post("/api/v1/executors/{id}/executions/{executionId}/renew",
                        executor, claimed.executionId())
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"leaseVersion":%d}
                                """.formatted(claimed.fencingToken())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.executionId").value(claimed.executionId()))
                .andExpect(jsonPath("$.leaseUntil").isNotEmpty());

        mockMvc.perform(post("/api/v1/executors/{id}/executions/{executionId}/renew",
                        executor, claimed.executionId())
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"leaseVersion":%d}
                                """.formatted(claimed.fencingToken() + 99)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("LEASE_VERSION_STALE"));
    }

    @Test
    void answersAResultForAnUnknownAttemptWithNotFound() throws Exception {
        long executor = register("reporter", "reporter-1");

        mockMvc.perform(post("/api/v1/executors/{id}/executions/{executionId}/result",
                        executor, 999999)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"leaseVersion":1,"outcome":"SUCCEEDED"}
                                """))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EXECUTION_NOT_FOUND"));
    }

    // ------------------------------------------------------------------ helpers

    private static final java.time.Duration TIMEOUT = java.time.Duration.ofSeconds(15);

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

    private Claimed claim(long executorId) throws Exception {
        String body = mockMvc.perform(post("/api/v1/executors/{id}/tasks/claim", executorId)
                        .header("X-ReactorSched-Token", EXECUTOR_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"batchSize": 10}
                                """))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        var tasks = objectMapper.readTree(body);
        assertThat(tasks).hasSize(1);
        var task = tasks.get(0);
        return new Claimed(task.get("executionId").asLong(), task.get("fencingToken").asLong());
    }

    private record Claimed(long executionId, long fencingToken) {
    }
}
