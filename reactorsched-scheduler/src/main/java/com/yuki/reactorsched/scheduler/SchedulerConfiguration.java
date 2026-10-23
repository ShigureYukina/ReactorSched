package com.yuki.reactorsched.scheduler;

import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Wires the scheduler: the clock probe always (it backs the health indicator and the startup
 * check), the scanner and the health indicator only when enabled through
 * {@code reactorsched.scheduler.enabled}.
 */
@Profile("!executor")
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfiguration {

    @Bean
    public DatabaseClockProbe databaseClockProbe(com.yuki.reactorsched.core.mapper.JobDefinitionMapper jobDefinitionMapper) {
        return new DatabaseClockProbe(jobDefinitionMapper);
    }

    @Bean
    @ConditionalOnProperty(name = "reactorsched.scheduler.enabled", havingValue = "true", matchIfMissing = true)
    public ScheduleScanner scheduleScanner(JobSchedulerService schedulerService,
                                           SchedulerProperties properties,
                                           DatabaseClockProbe clockProbe,
                                           ObjectProvider<ReactorMetrics> metrics) {
        schedulerService.attachMetrics(metrics.getIfAvailable());
        return new ScheduleScanner(schedulerService, properties, clockProbe);
    }

    @Bean
    @ConditionalOnProperty(name = "reactorsched.scheduler.enabled", havingValue = "true", matchIfMissing = true)
    public HealthIndicator schedulerHealthIndicator(DatabaseClockProbe clockProbe,
                                                    SchedulerProperties properties,
                                                    ScheduleScanner scanner) {
        return new SchedulerClockHealthIndicator(clockProbe, properties, scanner);
    }
}
