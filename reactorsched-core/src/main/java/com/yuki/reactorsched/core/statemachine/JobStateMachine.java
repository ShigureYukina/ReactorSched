package com.yuki.reactorsched.core.statemachine;

import com.yuki.reactorsched.core.domain.JobStatus;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The single definition of legal job state transitions (PRD 4.2).
 *
 * <p>Deliberately a separate class from {@link TaskStateMachine}: the two lifecycles have nothing
 * in common, and letting one answer for the other is how "is it enabled?" gets mistaken for "did it
 * run successfully?".
 *
 * <p>Every transition is validated here first and then defended again by a conditional SQL update,
 * so a concurrent operator or scheduler can never skip the state machine.
 */
public final class JobStateMachine {

    private static final Map<JobStatus, Set<JobStatus>> ALLOWED = allowedTransitions();

    private JobStateMachine() {
    }

    private static Map<JobStatus, Set<JobStatus>> allowedTransitions() {
        Map<JobStatus, Set<JobStatus>> allowed = new EnumMap<>(JobStatus.class);
        allowed.put(JobStatus.ENABLED, EnumSet.of(JobStatus.PAUSED, JobStatus.DELETED));
        allowed.put(JobStatus.PAUSED, EnumSet.of(JobStatus.ENABLED, JobStatus.DELETED));
        allowed.put(JobStatus.DELETED, EnumSet.noneOf(JobStatus.class));
        return Collections.unmodifiableMap(allowed);
    }

    public static boolean canTransition(JobStatus from, JobStatus to) {
        if (from == null || to == null) {
            return false;
        }
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }

    /**
     * @throws IllegalJobTransitionException when the transition is not part of the state machine
     */
    public static void requireTransition(JobStatus from, JobStatus to) {
        if (!canTransition(from, to)) {
            throw new IllegalJobTransitionException(from, to);
        }
    }

    public static Set<JobStatus> allowedTargets(JobStatus from) {
        return ALLOWED.getOrDefault(from, Set.of());
    }
}
