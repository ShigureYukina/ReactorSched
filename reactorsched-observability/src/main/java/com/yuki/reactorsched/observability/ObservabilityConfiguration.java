package com.yuki.reactorsched.observability;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Wiring for the observability module.
 *
 * <p>Its mapper lives in this module's own package, so it is scanned here instead of relying on the
 * mapper scan of {@code task-core}.
 */
@Profile("!executor")
@Configuration
@MapperScan("com.yuki.reactorsched.observability.mapper")
public class ObservabilityConfiguration {
}
