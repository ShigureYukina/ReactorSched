package com.yuki.reactorsched.core.domain;

/**
 * Names of the executor groups task instances are routed to.
 *
 * <p>The group is part of the claim's {@code WHERE} clause (PRD 7.2), so the scheduling center and
 * every executor must agree on the spelling. {@link #DEFAULT} is what an instance gets when its
 * definition does not name a group, and what the embedded {@code LOCAL} executor serves (PRD 7.5).
 */
public final class ExecutorGroups {

    /** Group of instances whose definition did not name one, and of the single-machine deployment. */
    public static final String DEFAULT = "DEFAULT";

    private ExecutorGroups() {
    }
}
