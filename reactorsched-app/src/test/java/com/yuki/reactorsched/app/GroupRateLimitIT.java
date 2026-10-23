package com.yuki.reactorsched.app;

import com.yuki.reactorsched.app.task.TestHandlersConfiguration;
import com.yuki.reactorsched.core.domain.TaskStatus;
import com.yuki.reactorsched.core.retry.RetryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Group-level retry admission (PRD 5.4, 12.3): retries must not crowd out fresh work, and the
 * per-second cap is shared by every executor of the group through {@code group_rate_window} - a
 * conditional UPDATE against MySQL, not a process-local counter (PRD 12.3 forbids passing one off
 * as global).
 */
@Import(TestHandlersConfiguration.class)
class GroupRateLimitIT extends AbstractIntegrationTest {

    @Test
    void pacesRetryDispatchWithoutStarvingTheQueue() {
        RetryProperties retry = retryWith(Duration.ofMillis(100), 0.0);
        retry.setMaxDispatchesPerSecond(1);
        startWorker(workerProperties(2), retry);

        List<Long> taskIds = IntStream.range(0, 3)
                .mapToObj(index -> submit("scenario", "{\"failures\": 1}").task().id())
                .toList();

        long startedAt = System.nanoTime();
        for (long taskId : taskIds) {
            awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(60));
            assertThat(countExecutions(taskId)).isEqualTo(2);
        }
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertThat(elapsedMillis)
                .as("without the admission limit the three retries would be dispatched within ~300ms")
                .isGreaterThanOrEqualTo(1000L);
    }

    @Test
    void twoWorkersShareTheSamePerSecondCap() {
        // A cap of 1/s for the whole group: with a shared window four retries need at least three
        // seconds. If each process counted for itself, two idle workers would dispatch two retries
        // per second and finish in about two.
        RetryProperties retry = retryWith(Duration.ofMillis(100), 0.0);
        retry.setMaxDispatchesPerSecond(1);
        startWorker(workerProperties(2), retry);
        startWorker(workerProperties(2), retry);

        List<Long> taskIds = IntStream.range(0, 4)
                .mapToObj(index -> submit("scenario", "{\"failures\": 1}").task().id())
                .toList();

        long startedAt = System.nanoTime();
        for (long taskId : taskIds) {
            awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(90));
        }
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertThat(elapsedMillis)
                .as("the per-second cap must span both workers through the shared group_rate_window row")
                .isGreaterThanOrEqualTo(3000L);
        // Every retry of the group passed through the same window row.
        Long windows = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM group_rate_window WHERE executor_group = 'DEFAULT' AND window_second > 0",
                Long.class);
        assertThat(windows).isGreaterThan(0L);
    }
}
