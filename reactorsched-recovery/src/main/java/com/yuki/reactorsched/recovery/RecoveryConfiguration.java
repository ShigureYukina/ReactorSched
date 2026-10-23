package com.yuki.reactorsched.recovery;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Wires the lease recovery scanner unless it is disabled through {@code reactorsched.recovery.enabled}.
 */
@Profile("!executor")
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RecoveryProperties.class)
public class RecoveryConfiguration {

    @Bean
    @ConditionalOnProperty(name = "reactorsched.recovery.enabled", havingValue = "true", matchIfMissing = true)
    public LeaseRecoveryScanner leaseRecoveryScanner(LeaseRecoveryService recoveryService,
                                                     RecoveryProperties properties) {
        return new LeaseRecoveryScanner(recoveryService, properties);
    }
}
