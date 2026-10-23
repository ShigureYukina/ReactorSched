package com.yuki.reactorsched.executor.api;

/**
 * Extension point for business logic (PRD 8.3).
 *
 * <p>Adding a task type means adding one bean of this type — the scheduling core never changes. A
 * handler reports failure by throwing an exception.
 *
 * <p>This interface is the whole contract an executor process compiles against: it carries no
 * Spring, MyBatis or JDBC type, so an executor can run in a plain JVM next to an application that
 * has nothing to do with the scheduling center.
 */
public interface TaskHandler {

    /** Task type this handler serves, for example {@code echo}. Registered types are compared case-insensitively. */
    String taskType();

    /**
     * Executes one attempt of the task.
     *
     * @param context read-only view of the task and its attempt number
     * @throws Exception when the attempt fails
     */
    void execute(TaskContext context) throws Exception;
}
