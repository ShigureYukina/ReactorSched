package com.yuki.reactorsched.executor.sdk;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The bounded pool of PRD 7.4: fixed size, {@code ArrayBlockingQueue}, explicit {@code AbortPolicy}.
 *
 * <p>The queue bound is what makes "claim no more than the free slots" a safety net rather than the
 * only defense: even if a claim asked for too much, the pool refuses the excess instead of growing
 * without bound.
 */
class ExecutionPoolTest {

    @Test
    void queueIsBoundedAndRejectionIsExplicit() throws Exception {
        ThreadPoolExecutor pool = ExecutorEngine.newExecutionPool(2, 1);
        CountDownLatch running = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger started = new AtomicInteger();
        try {
            assertThat(pool.getQueue()).isInstanceOf(java.util.concurrent.ArrayBlockingQueue.class);
            assertThat(pool.getCorePoolSize()).isEqualTo(2);
            assertThat(pool.getMaximumPoolSize()).isEqualTo(2);

            for (int i = 0; i < 2; i++) {
                pool.execute(() -> {
                    started.incrementAndGet();
                    running.countDown();
                    await(release);
                });
            }
            assertThat(running.await(2, TimeUnit.SECONDS)).isTrue();
            // The third task has nowhere to run yet, so the bounded queue takes it.
            pool.execute(() -> {
                started.incrementAndGet();
                await(release);
            });
            assertThat(pool.getQueue()).hasSize(1);

            // Both threads busy and the queue full: the next submission is aborted, not queued.
            assertThatThrownBy(() -> pool.execute(() -> started.incrementAndGet()))
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
        } finally {
            release.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void abandonsQueuedWorkOnlyWhenForced() throws Exception {
        ThreadPoolExecutor pool = ExecutorEngine.newExecutionPool(1, 1);
        try {
            CountDownLatch running = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            pool.execute(() -> {
                running.countDown();
                await(release);
            });
            assertThat(running.await(2, TimeUnit.SECONDS)).isTrue();

            // shutdown() lets the queued task run; only shutdownNow() abandons it.
            CountDownLatch queued = new CountDownLatch(1);
            pool.execute(queued::countDown);
            pool.shutdown();
            release.countDown();
            Awaitility.await("queued task runs after shutdown")
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(queued.getCount()).isZero());
        } finally {
            pool.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch was not released in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
