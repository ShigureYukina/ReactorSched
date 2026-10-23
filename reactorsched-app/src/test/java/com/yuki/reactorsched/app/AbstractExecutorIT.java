package com.yuki.reactorsched.app;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * Base class for the executor-facing integration tests (PRD 7).
 *
 * <p>The application serves real HTTP on a random port, so the executor endpoints are exercised
 * exactly the way a remote executor calls them: through the security filter, the controller and the
 * services, down to MySQL. The shared executor token is the development default from
 * {@code application.yml}; a test that needs other executor settings overrides them through its own
 * {@code @DynamicPropertySource}, which gives the class its own Spring context.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractExecutorIT extends AbstractIntegrationTest {

    /** Development default of {@code reactorsched.executor.token} (application.yml). */
    protected static final String EXECUTOR_TOKEN = "reactor-executor-dev";

    @LocalServerPort
    protected int port;

    /** Base URL of the randomly-ported application, as an executor process would configure it. */
    protected String baseUrl() {
        return "http://localhost:" + port;
    }
}
