package com.yuki.reactorsched.core.statemachine;

import com.yuki.reactorsched.core.domain.JobStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The definition state machine is fixed by PRD 4.2: every legal transition is allowed, every other
 * pair is refused. The table is enumerated explicitly so adding a transition is a deliberate edit,
 * not an accident of an enum switch.
 */
class JobStateMachineTest {

    @Test
    @DisplayName("allowed transitions match PRD 4.2 exactly")
    void allowedTransitions() {
        assertThat(JobStateMachine.canTransition(JobStatus.ENABLED, JobStatus.PAUSED)).isTrue();
        assertThat(JobStateMachine.canTransition(JobStatus.ENABLED, JobStatus.DELETED)).isTrue();
        assertThat(JobStateMachine.canTransition(JobStatus.PAUSED, JobStatus.ENABLED)).isTrue();
        assertThat(JobStateMachine.canTransition(JobStatus.PAUSED, JobStatus.DELETED)).isTrue();
    }

    @Test
    @DisplayName("deleted is terminal and no job can stay in its own state")
    void refusedTransitions() {
        for (JobStatus target : JobStatus.values()) {
            assertThat(JobStateMachine.canTransition(JobStatus.DELETED, target)).isFalse();
        }
        for (JobStatus status : JobStatus.values()) {
            assertThat(JobStateMachine.canTransition(status, status)).isFalse();
        }
    }

    @Test
    void requireTransitionThrowsWithBothEnds() {
        assertThatThrownBy(() -> JobStateMachine.requireTransition(JobStatus.DELETED, JobStatus.ENABLED))
                .isInstanceOf(IllegalJobTransitionException.class)
                .hasMessageContaining("DELETED")
                .hasMessageContaining("ENABLED");
    }

    @Test
    void nullsAreNeverAllowed() {
        assertThat(JobStateMachine.canTransition(null, JobStatus.ENABLED)).isFalse();
        assertThat(JobStateMachine.canTransition(JobStatus.ENABLED, null)).isFalse();
    }
}
