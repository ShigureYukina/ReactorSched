package com.yuki.reactorsched.app.task;

import com.yuki.reactorsched.executor.api.TaskContext;
import com.yuki.reactorsched.executor.api.TaskHandler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test only task type for the executor-takeover scenario: the first attempt blocks like a long
 * business call, every later attempt succeeds immediately.
 *
 * <p>That shape is what a killed executor leaves behind: it claimed the instance and was executing
 * it when the process died. After lease recovery the instance comes back, the attempt number is
 * higher, and the surviving executor's run is the one that finishes.
 */
public class TakeoverTaskHandler implements TaskHandler {

    public static final String TASK_TYPE = "takeover";

    private static final AtomicInteger FIRST_ATTEMPT_ENTRIES = new AtomicInteger();

    public static void reset() {
        FIRST_ATTEMPT_ENTRIES.set(0);
    }

    /** Whether some executor entered the first attempt of a takeover task. */
    public static boolean firstAttemptEntered() {
        return FIRST_ATTEMPT_ENTRIES.get() > 0;
    }

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public void execute(TaskContext context) throws Exception {
        if (context.attempt() == 1) {
            FIRST_ATTEMPT_ENTRIES.incrementAndGet();
            // Interruptible like any blocking downstream call: the watchdog, a lost lease or a
            // shutdown all express themselves as an interrupt (PRD 8.3).
            Thread.sleep(60_000);
            return;
        }
        context.logger().log("attempt " + context.attempt() + " took the instance over");
    }
}
