package com.yuki.reactorsched.executor.registry;

import com.yuki.reactorsched.executor.registry.properties.ExecutorProperties;
import com.yuki.reactorsched.executor.registry.service.ExecutorRegistryService;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Wiring of the scheduling center's executor-facing side (PRD 7).
 *
 * <p>The mappers live beside the domain they persist and are scanned here, so a module that brings
 * the registry brings its own SQL too. The offline sweep is a plain {@link Bean} rather than a
 * {@code @Component} so that a test which only exercises the services can leave it out.
 */
@Profile("!executor")
@Configuration
@MapperScan("com.yuki.reactorsched.executor.registry.mapper")
@EnableConfigurationProperties(ExecutorProperties.class)
public class ExecutorRegistryConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ExecutorRegistryConfiguration.class);

    /**
     * The shared executor token is the only thing standing between the open internet and the
     * executor endpoints (PRD 10.3). A blank token would make every executor call fail at runtime
     * with a confusing 401, so the center refuses to start instead.
     */
    @Bean
    public ExecutorOfflineScanner executorOfflineScanner(ExecutorRegistryService registryService,
                                                         ExecutorProperties properties,
                                                         ObjectProvider<ReactorMetrics> metrics) {
        String token = properties.getToken();
        if (token == null || token.isBlank()) {
            throw new IllegalStateException(
                    "reactorsched.executor.token must be set: executors authenticate with this shared secret");
        }
        log.info("executor registry enabled: heartbeatInterval={}s offlineThreshold={}s leaseTtl={}s",
                properties.getHeartbeatInterval().toSeconds(),
                properties.effectiveOfflineThreshold().toSeconds(),
                properties.getLeaseTtl().toSeconds());
        ExecutorOfflineScanner scanner = new ExecutorOfflineScanner(registryService, properties);
        scanner.attachMetrics(metrics.getIfAvailable());
        return scanner;
    }
}
