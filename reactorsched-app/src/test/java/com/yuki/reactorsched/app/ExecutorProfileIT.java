package com.yuki.reactorsched.app;

import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.executor.sdk.ExecutorEngine;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * The executor role (PRD 12.5): one artifact, started with the {@code executor} profile, runs the
 * SDK only and never opens a database connection.
 *
 * <p>No test container is started here on purpose: if this context needed MySQL, the executor role
 * would not be deployable without the scheduling center's database.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active=executor")
class ExecutorProfileIT {

    @Autowired
    private ApplicationContext context;

    @Test
    void startsTheSdkAndNothingThatTouchesTheDatabase() {
        assertThatNoException().isThrownBy(() -> context.getBean(ExecutorEngine.class));
        assertThat(context.getBeanNamesForType(TaskMapper.class)).isEmpty();
    }

    @Test
    void exposesHealthForTheMonitoringStack() {
        // The executor process keeps the web server for /actuator (health + prometheus); the
        // monitoring stack scrapes it like any other target (PRD 8.6).
        assertThat(context.getEnvironment().getProperty("server.port")).isNotNull();
    }
}
