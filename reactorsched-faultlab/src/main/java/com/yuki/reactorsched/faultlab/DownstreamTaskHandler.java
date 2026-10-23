package com.yuki.reactorsched.faultlab;

import com.yuki.reactorsched.executor.api.OverloadedTaskException;
import com.yuki.reactorsched.executor.api.ThrottledTaskException;
import com.yuki.reactorsched.executor.api.TransientTaskException;
import com.yuki.reactorsched.executor.api.TaskContext;
import com.yuki.reactorsched.executor.api.TaskHandler;
import com.yuki.reactorsched.core.signals.RuntimeSignals;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Task type {@code downstream}: performs one call against the mock downstream and reports the
 * outcome as the matching task failure. Success and failure are both recorded in the runtime
 * signals, which is what the adaptive controller reacts to.
 */
@Component
public class DownstreamTaskHandler implements TaskHandler {

    public static final String TASK_TYPE = "downstream";

    private final MockDownstream downstream;
    private final RuntimeSignals signals;

    public DownstreamTaskHandler(MockDownstream downstream, RuntimeSignals signals) {
        this.downstream = downstream;
        this.signals = signals;
    }

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public void execute(TaskContext context) {
        DownstreamResult result = downstream.call();
        signals.recordDownstreamCall(result.latencyMillis(), result.failed());
        if (!result.failed()) {
            return;
        }
        throw switch (result.kind()) {
            case OVERLOADED -> new OverloadedTaskException(result.detail());
            case THROTTLED -> new ThrottledTaskException(result.detail(),
                    Duration.ofMillis(result.retryAfterMillis()));
            case TRANSIENT -> new TransientTaskException(result.detail());
        };
    }
}
