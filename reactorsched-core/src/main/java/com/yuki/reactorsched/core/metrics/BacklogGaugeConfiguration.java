package com.yuki.reactorsched.core.metrics;

import com.yuki.reactorsched.core.mapper.TaskMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Clock;

/**
 * The periodic refresh of the two cluster-wide gauges (PRD 8.6).
 *
 * <p>Only in processes that talk to the database: the gauges describe the whole cluster's backlog
 * and running attempts, so they must be computed from the shared tables. An executor process (the
 * {@code executor} profile) has no database connection by design and skips this configuration; its
 * meters are the counters and timers it publishes itself.
 */
@Configuration
@Profile("!executor")
public class BacklogGaugeConfiguration {

    @Bean
    public BacklogGaugeRefresher backlogGaugeRefresher(TaskMapper taskMapper, ReactorMetrics metrics,
                                                       Clock clock) {
        return new BacklogGaugeRefresher(taskMapper, metrics, clock, 5_000L);
    }
}
