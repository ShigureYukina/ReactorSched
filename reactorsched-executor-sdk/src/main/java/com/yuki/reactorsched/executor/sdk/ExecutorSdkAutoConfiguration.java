package com.yuki.reactorsched.executor.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.executor.api.TaskHandler;
import com.yuki.reactorsched.executor.api.TaskHandlerRegistry;
import com.yuki.reactorsched.executor.sdk.client.ExecutorGateway;
import com.yuki.reactorsched.executor.sdk.client.RestExecutorGateway;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Clock;
import java.util.List;

/**
 * Wiring of an executor process, imported by {@link EnableReactorSchedExecutor}.
 *
 * <p>An {@link AutoConfiguration} rather than a plain {@code @Configuration} on purpose: a
 * scheduling center that carries the SDK on its classpath must not silently become an executor, and
 * an executor that carries the center's modules must not inherit the center's handler registry. Only
 * the annotation (or an explicit import) turns the configuration on.
 *
 * <p>The handler registry is built from the discovered {@link TaskHandler} beans right here: an
 * executor process does not run the scheduling center, so it must not depend on the core
 * configuration for its own SPI (AGENTS.md 2).
 */
@AutoConfiguration
@EnableConfigurationProperties(ExecutorSdkProperties.class)
public class ExecutorSdkAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ExecutorGateway.class)
    public ExecutorGateway executorGateway(ExecutorSdkProperties properties, ObjectMapper objectMapper) {
        return new RestExecutorGateway(properties, objectMapper);
    }

    @Bean
    public TaskHandlerRegistry taskHandlerRegistry(List<TaskHandler> handlers) {
        return new TaskHandlerRegistry(handlers);
    }

    /**
     * The engine. It is a plain bean so an application that wants to control when the executor
     * starts can wrap it; by default the {@code SmartLifecycle} phase starts it with the context.
     *
     * <p>The clock falls back to UTC when the application brings no bean of its own - an executor
     * process does not depend on the core module, which is where the shared clock lives.
     */
    @Bean
    public ExecutorEngine executorEngine(ExecutorGateway gateway, TaskHandlerRegistry handlerRegistry,
                                         ExecutorSdkProperties properties, ObjectMapper objectMapper,
                                         ObjectProvider<Clock> clockProvider) {
        return new ExecutorEngine(gateway, handlerRegistry, properties, objectMapper,
                clockProvider.getIfAvailable(Clock::systemUTC));
    }
}
