package com.yuki.reactorsched.executor.sdk;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Turns a Spring Boot application into a ReactorSched executor process (PRD 7).
 *
 * <p>The annotated application provides {@code TaskHandler} beans; the SDK registers with the
 * configured scheduling center, heartbeats, pulls work, runs it in a bounded pool, renews leases
 * and reports results. Nothing else is required - and nothing of the scheduling center's modules
 * is pulled in, so an executor stays a small process next to the application it serves.
 *
 * <pre>
 * &#64;SpringBootApplication
 * &#64;EnableReactorSchedExecutor
 * public class MyExecutorApplication {
 *     &#64;Bean TaskHandler reportHandler() { ... }
 * }
 * </pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(ExecutorSdkAutoConfiguration.class)
public @interface EnableReactorSchedExecutor {
}
