package com.yuki.reactorsched.app.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.yuki.reactorsched.executor.api.InvalidTaskParameterException;
import com.yuki.reactorsched.executor.api.OverloadedTaskException;
import com.yuki.reactorsched.executor.api.PermanentTaskException;
import com.yuki.reactorsched.executor.api.ThrottledTaskException;
import com.yuki.reactorsched.executor.api.TransientTaskException;
import com.yuki.reactorsched.executor.api.TaskContext;
import com.yuki.reactorsched.executor.api.TaskHandler;

import java.time.Duration;

/**
 * Test only task type that produces a scripted failure sequence, so the retry classification and the
 * backoff behaviour can be asserted without inventing production handlers.
 *
 * <p>Payload:
 * <ul>
 *   <li>{@code failures}: how many attempts fail (default 0, i.e. the task always succeeds)</li>
 *   <li>{@code failureKind}: {@code transient} (default), {@code throttled}, {@code overloaded},
 *       {@code rejection} or {@code invalid-parameter}</li>
 *   <li>{@code retryAfterMs}: hint carried by a {@code throttled} failure</li>
 *   <li>{@code sleepMs}: optional delay before the outcome is decided</li>
 * </ul>
 */
public class ScenarioTaskHandler implements TaskHandler {

    public static final String TASK_TYPE = "scenario";

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public void execute(TaskContext context) throws Exception {
        JsonNode payload = context.payload();
        long sleepMs = payload.path("sleepMs").asLong(0L);
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }

        int failures = payload.path("failures").asInt(0);
        if (context.attempt() > failures) {
            return;
        }

        String kind = payload.path("failureKind").asText("transient");
        String message = "scenario failure (" + kind + ") on attempt " + context.attempt();
        throw switch (kind) {
            case "transient" -> new TransientTaskException(message);
            case "throttled" -> new ThrottledTaskException(
                    message, Duration.ofMillis(payload.path("retryAfterMs").asLong(1000L)));
            case "overloaded" -> new OverloadedTaskException(message);
            case "rejection" -> new PermanentTaskException(message);
            case "invalid-parameter" -> new InvalidTaskParameterException(message);
            default -> new IllegalArgumentException("unknown failure kind: " + kind);
        };
    }
}
