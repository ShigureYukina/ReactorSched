package com.yuki.reactorsched.adaptive;

import com.yuki.reactorsched.core.concurrency.ConcurrencyLimit;
import com.yuki.reactorsched.core.concurrency.GroupConcurrencyStore;
import com.yuki.reactorsched.core.mapper.GroupRateWindowMapper;
import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import com.yuki.reactorsched.core.signals.RuntimeSignals;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Clock;

/**
 * Wires the adaptive controller only when {@code reactorsched.adaptive.enabled} is set. Without it the
 * worker falls back to its configured fixed concurrency, so the first two phases keep their
 * semantics.
 */
@Profile("!executor")
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AdaptiveProperties.class)
public class AdaptiveConfiguration {

    @Bean
    @ConditionalOnProperty(name = "reactorsched.adaptive.enabled", havingValue = "true")
    public ConcurrencyLimit concurrencyLimit(AdaptiveProperties properties) {
        properties.validate();
        return new ConcurrencyLimit(properties.getMinConcurrency(), properties.getMaxConcurrency(),
                properties.getInitialConcurrency());
    }

    @Bean
    @ConditionalOnProperty(name = "reactorsched.adaptive.enabled", havingValue = "true")
    public AimdController aimdController(AdaptiveProperties properties, ConcurrencyLimit concurrencyLimit,
                                         RuntimeSignals runtimeSignals, TaskMapper taskMapper,
                                         ConcurrencyDecisionRecorder recorder,
                                         Clock clock, GroupRateWindowMapper rateWindowMapper,
                                         ObjectProvider<ReactorMetrics> metrics) {
        AimdController controller = new AimdController(properties, concurrencyLimit, runtimeSignals,
                taskMapper, recorder, clock, null, new GroupConcurrencyStore(rateWindowMapper));
        controller.attachMetrics(metrics.getIfAvailable());
        return controller;
    }
}
