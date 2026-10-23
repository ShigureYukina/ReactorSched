package com.yuki.reactorsched.app;

import com.yuki.reactorsched.executor.sdk.ExecutorSdkAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

/**
 * Reactor: persistent asynchronous task execution engine.
 *
 * <p>This module only wires modules together; behaviour lives in the core, api, worker, scheduler
 * and executor modules. One artifact runs in two roles:
 *
 * <ul>
 *   <li><b>scheduling center</b> (default profile): API, scheduler, worker, registry, recovery,
 *       observability. Several instances can run side by side; the optimistic-lock claim keeps one
 *       fire time to one instance (PRD 6.2).</li>
 *   <li><b>executor</b> (the {@code executor} profile): the SDK only - registration, heartbeat,
 *       claiming and result reporting over REST, with no database connection at all. The
 *       scheduling-center components are excluded by {@code @Profile("!executor")}, and the SDK is
 *       imported here rather than through an auto-configuration import so a center that carries the
 *       SDK on its classpath never silently becomes an executor (AGENTS.md 6).</li>
 * </ul>
 */
@SpringBootApplication(scanBasePackages = "com.yuki.reactorsched")
@ConfigurationPropertiesScan("com.yuki.reactorsched")
public class ReactorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactorApplication.class, args);
    }

    /** The executor role: SDK and handlers, nothing that touches the database. */
    @Configuration
    @Profile("executor")
    @Import(ExecutorSdkAutoConfiguration.class)
    static class ExecutorRole {
    }
}
