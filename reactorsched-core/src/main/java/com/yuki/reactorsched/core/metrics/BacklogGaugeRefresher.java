package com.yuki.reactorsched.core.metrics;

import com.yuki.reactorsched.core.mapper.TaskMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Refreshes the two cluster-wide gauges ({@code reactorsched.backlog.due}, {@code reactorsched.running})
 * from the database, because an in-process counter would under-report: schedulers and executors
 * share one database, and every instance would only see its own work.
 *
 * <p>The refresh is deliberately slow (every few seconds) and quiet: a failed read keeps the last
 * value instead of zeroing it, so a transient database error cannot fake a cleared backlog.
 */
public class BacklogGaugeRefresher implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(BacklogGaugeRefresher.class);

    private final TaskMapper taskMapper;
    private final ReactorMetrics metrics;
    private final Clock clock;
    private final long intervalMillis;

    private ScheduledExecutorService scheduler;

    public BacklogGaugeRefresher(TaskMapper taskMapper, ReactorMetrics metrics, Clock clock, long intervalMillis) {
        this.taskMapper = taskMapper;
        this.metrics = metrics;
        this.clock = clock;
        this.intervalMillis = Math.max(500L, intervalMillis);
    }

    @Override
    public void afterPropertiesSet() {
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "reactorsched-metrics-backlog");
            thread.setDaemon(false);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::refreshQuietly, intervalMillis, intervalMillis,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private void refreshQuietly() {
        try {
            LocalDateTime now = LocalDateTime.now(clock);
            metrics.backlog(taskMapper.countDueBacklog(now), taskMapper.countRunning());
        } catch (Exception e) {
            log.warn("could not refresh the backlog gauges; keeping the last published values", e);
        }
    }
}
