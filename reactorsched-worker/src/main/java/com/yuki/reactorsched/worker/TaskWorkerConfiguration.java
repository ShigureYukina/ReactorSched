package com.yuki.reactorsched.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.core.concurrency.ConcurrencyLimit;
import com.yuki.reactorsched.core.decision.SchedulerDecisionRecorder;
import com.yuki.reactorsched.core.mapper.GroupRateWindowMapper;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import com.yuki.reactorsched.executor.api.TaskHandlerRegistry;
import com.yuki.reactorsched.core.retry.RetryProperties;
import com.yuki.reactorsched.core.service.TaskOperations;
import com.yuki.reactorsched.core.signals.RuntimeSignals;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Clock;

/**
 * Registers the worker engine unless {@code reactorsched.worker.enabled=false}.
 *
 * <p>Disabling the worker is how a process runs as API-only, and how tests keep control over who
 * executes tasks.
 *
 * <p>The concurrency limit is looked up instead of required: without an adaptive controller the
 * worker uses a fixed limit from its own configuration.
 */
@Profile("!executor")
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TaskWorkerProperties.class)
public class TaskWorkerConfiguration {

    @Bean
    @ConditionalOnProperty(name = "reactorsched.worker.enabled", havingValue = "true", matchIfMissing = true)
    public TaskWorkerEngine taskWorkerEngine(TaskOperations taskOperations,
                                            TaskHandlerRegistry handlerRegistry,
                                            TaskWorkerProperties properties,
                                            RetryProperties retryProperties,
                                            RuntimeSignals runtimeSignals,
                                            ObjectProvider<ConcurrencyLimit> concurrencyLimit,
                                            ObjectMapper objectMapper,
                                            Clock clock,
                                            SchedulerDecisionRecorder decisionRecorder,
                                            GroupRateWindowMapper rateWindowMapper,
                                            ObjectProvider<ReactorMetrics> metrics) {
        TaskWorkerEngine engine = new TaskWorkerEngine(taskOperations, handlerRegistry, properties, retryProperties,
                runtimeSignals, concurrencyLimit.getIfAvailable(), objectMapper, clock, null,
                decisionRecorder, rateWindowMapper);
        engine.attachMetrics(metrics.getIfAvailable());
        return engine;
    }
}
