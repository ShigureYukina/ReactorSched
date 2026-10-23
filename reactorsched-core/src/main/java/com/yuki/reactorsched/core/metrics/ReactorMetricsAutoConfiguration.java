package com.yuki.reactorsched.core.metrics;

import com.yuki.reactorsched.core.mapper.TaskMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for the meters of PRD 8.6.
 *
 * <p>An auto-configuration rather than a scanned {@code @Configuration} so it is loaded in every
 * process that uses the core (scheduling center and executor alike) without any module having to
 * import it.
 *
 * <p>The registry is looked up at runtime rather than through {@code @ConditionalOnBean}: that
 * condition only matches bean definitions registered before this configuration is evaluated, and
 * the registry comes from Spring Boot's own metrics auto-configurations, whose order relative to
 * this one is not something to bet the dashboard on. When no registry is present (a process without
 * an actuator, a plain unit test context) the meters fall back to a private in-memory registry:
 * nobody scrapes them there, and every call site treats the bean as "publish if you can" rather
 * than failing.
 */
@AutoConfiguration
@ConditionalOnClass(MeterRegistry.class)
public class ReactorMetricsAutoConfiguration {

    @Bean
    public ReactorMetrics reactorMetrics(ObjectProvider<MeterRegistry> meterRegistry) {
        MeterRegistry registry = meterRegistry.getIfAvailable();
        return new ReactorMetrics(registry == null ? new SimpleMeterRegistry() : registry);
    }
}
