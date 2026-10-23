package com.yuki.reactorsched.core;

import com.yuki.reactorsched.core.backpressure.BackpressureGuard;
import com.yuki.reactorsched.core.backpressure.BackpressureProperties;
import com.yuki.reactorsched.core.decision.SchedulerDecisionRecorder;
import com.yuki.reactorsched.core.mapper.TaskMapper;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import com.yuki.reactorsched.core.retry.RetryPolicy;
import com.yuki.reactorsched.core.retry.RetryProperties;
import com.yuki.reactorsched.executor.api.TaskHandler;
import com.yuki.reactorsched.executor.api.TaskHandlerRegistry;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Clock;
import java.util.List;
import java.util.Random;
import java.util.random.RandomGenerator;

/**
 * Wiring shared by every module that uses the task domain.
 */
@Profile("!executor")
@Configuration
@MapperScan("com.yuki.reactorsched.core.mapper")
@EnableConfigurationProperties({RetryProperties.class, BackpressureProperties.class})
public class ReactorCoreConfiguration {

    /**
     * All persisted timestamps come from this clock and are UTC (AGENTS.md 3). Injecting it keeps
     * tests and production code on the same notion of "now" instead of scattered system calls.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Source of the retry jitter. It is a bean so that an experiment can replace it with a seeded
     * generator and reproduce a run exactly (AGENTS.md 13).
     */
    @Bean
    public RandomGenerator randomGenerator() {
        return new Random();
    }

    /**
     * Exponential backoff with jitter, shared by every executor of this process (PRD 7).
     */
    @Bean
    public RetryPolicy retryPolicy(RetryProperties properties, RandomGenerator randomGenerator) {
        return new RetryPolicy(properties, randomGenerator);
    }

    /**
     * Handler registry of this process, built from the discovered {@link TaskHandler} beans. The SPI
     * itself lives in {@code reactorsched-executor-api}; the bean is wired here so the scheduling
     * center and an executor process can both start from a plain constructor (AGENTS.md 2).
     */
    @Bean
    public TaskHandlerRegistry taskHandlerRegistry(List<TaskHandler> handlers) {
        return new TaskHandlerRegistry(handlers);
    }

    /**
     * Admission check of the API entry points (PRD 8.5). Controllers call it before the service
     * method so the rejection decision row commits outside any business transaction.
     */
    @Bean
    public BackpressureGuard backpressureGuard(TaskMapper taskMapper, BackpressureProperties properties,
                                               SchedulerDecisionRecorder decisionRecorder, Clock clock) {
        return new BackpressureGuard(taskMapper, properties, decisionRecorder, clock);
    }

    /**
     * Attaches the decision counter to the recorder after startup (PRD 8.6 panel "decision reason
     * distribution"). The wiring runs as a singleton initializer rather than a constructor
     * injection: the recorder is component-scanned with a constructor the unit tests rely on, and
     * {@link ReactorMetrics} is an auto-configuration bean that may only be created after the
     * user configuration - the ordering trap documented in {@code ReactorMetricsAutoConfiguration}.
     * No registry on the classpath (or none published) simply leaves the counter off.
     */
    @Bean
    public SmartInitializingSingleton decisionCounterWiring(SchedulerDecisionRecorder decisionRecorder,
                                                            ObjectProvider<ReactorMetrics> metrics) {
        return () -> metrics.ifAvailable(decisionRecorder::attachMetrics);
    }
}
