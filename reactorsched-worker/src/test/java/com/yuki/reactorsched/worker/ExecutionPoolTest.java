package com.yuki.reactorsched.worker;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the shape of the execution pool (PRD 7.4): bounded queue, explicit rejection policy, never
 * one of the unbounded {@code Executors} factories.
 */
class ExecutionPoolTest {

    private static final ThreadFactory DAEMON = runnable -> {
        Thread thread = new Thread(runnable, "worker-pool-test");
        thread.setDaemon(true);
        return thread;
    };

    @Test
    void poolIsFixedWithBoundedQueueAndAbortPolicy() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) TaskWorkerEngine.newExecutionPool(2, 4, DAEMON);
        try {
            assertThat(pool.getCorePoolSize()).isEqualTo(2);
            assertThat(pool.getMaximumPoolSize()).isEqualTo(2);
            assertThat(pool.getQueue()).isInstanceOf(ArrayBlockingQueue.class);
            assertThat(pool.getQueue().remainingCapacity()).isEqualTo(4);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void fullQueueRejectsInsteadOfGrowingForever() throws InterruptedException {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) TaskWorkerEngine.newExecutionPool(1, 1, DAEMON);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch running = new CountDownLatch(1);
        try {
            pool.execute(() -> {
                running.countDown();
                await(release);
            });
            assertThat(running.await(2, TimeUnit.SECONDS)).isTrue();
            pool.execute(() -> { }); // fills the single queue slot
            assertThatThrownBy(() -> pool.execute(() -> { }))
                    .isInstanceOf(RejectedExecutionException.class);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
