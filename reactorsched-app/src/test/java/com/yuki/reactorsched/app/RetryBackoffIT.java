package com.yuki.reactorsched.app;

import com.yuki.reactorsched.app.task.ScenarioTaskHandler;
import com.yuki.reactorsched.app.task.TestHandlersConfiguration;
import com.yuki.reactorsched.core.domain.ErrorCodes;
import com.yuki.reactorsched.core.domain.ExecutionResult;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.core.dto.TaskExecutionView;
import com.yuki.reactorsched.core.dto.TaskView;
import com.yuki.reactorsched.executor.api.ErrorCategory;
import com.yuki.reactorsched.core.retry.RetryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Retry behaviour (PRD 5.4): retryable errors wait for the backoff window, retries are recorded per
 * attempt, and a downstream retry-after hint is obeyed.
 */
@Import(TestHandlersConfiguration.class)
class RetryBackoffIT extends AbstractIntegrationTest {

    @Test
    void retriesTransientFailuresUntilAnAttemptSucceeds() {
        startWorker(2);
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE, "{\"failures\": 2}").task().id();

        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));

        TaskView view = task(taskId);
        assertThat(view.attemptCount()).isEqualTo(3);
        assertThat(view.lastError()).as("a successful attempt clears the last error").isNull();

        List<TaskExecutionView> executions = executions(taskId);
        assertThat(executions).extracting(TaskExecutionView::attempt).containsExactly(1, 2, 3);
        assertThat(executions).extracting(TaskExecutionView::result)
                .containsExactly(ExecutionResult.FAILED, ExecutionResult.FAILED, ExecutionResult.SUCCEEDED);
        assertThat(executions).extracting(TaskExecutionView::errorCategory)
                .containsExactly(ErrorCategory.TRANSIENT, ErrorCategory.TRANSIENT, null);
        assertThat(executions.getFirst().errorCode()).isEqualTo(ErrorCodes.EXECUTION_ERROR);
        assertThat(executions.getFirst().errorMessage()).contains("scenario failure (transient) on attempt 1");
        assertThat(executions).extracting(TaskExecutionView::fencingToken)
                .as("every attempt owns a fresh fencing token")
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void waitsForTheBackoffWindowBeforeDispatchingTheRetry() {
        // A two second base delay without jitter makes the window observable.
        RetryProperties retry = retryWith(Duration.ofSeconds(2), 0.0);
        startWorker(workerProperties(1), retry);
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE, "{\"failures\": 1}").task().id();

        awaitUntil(() -> task(taskId).status() == TaskStatus.RETRY_WAIT, Duration.ofSeconds(10));

        TaskView waiting = task(taskId);
        assertThat(waiting.nextRunAt()).isAfter(LocalDateTime.now(clock));
        assertThat(waiting.lastError()).contains(ErrorCategory.TRANSIENT.name());
        assertThat(countExecutions(taskId)).as("the retry is not dispatched inside the backoff window").isEqualTo(1);

        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        assertThat(executions(taskId)).hasSize(2);
    }

    @Test
    void honoursTheRetryAfterHintOfAThrottledDownstream() {
        startWorker(workerProperties(1), retryWith(Duration.ofMillis(200), 0.0));
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE,
                "{\"failures\": 1, \"failureKind\": \"throttled\", \"retryAfterMs\": 3000}").task().id();

        awaitUntil(() -> task(taskId).status() == TaskStatus.RETRY_WAIT, Duration.ofSeconds(10));

        assertThat(task(taskId).nextRunAt())
                .as("the downstream hint wins over the 200 ms backoff of the engine")
                .isAfter(LocalDateTime.now(clock).plusSeconds(1));

        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        assertThat(executions(taskId).getFirst().errorCategory()).isEqualTo(ErrorCategory.THROTTLED);
    }
}
