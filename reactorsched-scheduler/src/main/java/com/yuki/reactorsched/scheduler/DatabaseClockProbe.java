package com.yuki.reactorsched.scheduler;

import com.yuki.reactorsched.core.mapper.JobDefinitionMapper;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Measures the drift between this node's clock and the database clock (PRD 6.5).
 *
 * <p>Every fire time decision is made against rows written with the database's clock, so a node
 * whose own clock has drifted fires the wrong instances. The measurement is deliberately taken
 * around the query and reported as one skew figure; the round trip is included, which makes the
 * number slightly pessimistic rather than optimistic.
 */
public class DatabaseClockProbe {

    private final JobDefinitionMapper jobDefinitionMapper;

    public DatabaseClockProbe(JobDefinitionMapper jobDefinitionMapper) {
        this.jobDefinitionMapper = jobDefinitionMapper;
    }

    public Measurement measure() {
        LocalDateTime nodeBefore = LocalDateTime.now();
        LocalDateTime databaseNow = jobDefinitionMapper.databaseNow();
        LocalDateTime nodeAfter = LocalDateTime.now();
        // The database answered somewhere inside the window; the middle of the window is the best
        // estimate of when it answered, and the window itself bounds the error.
        LocalDateTime nodeMid = nodeBefore.plusNanos(
                Duration.between(nodeBefore, nodeAfter).toNanos() / 2);
        long skewMillis = Duration.between(databaseNow, nodeMid).toMillis();
        return new Measurement(databaseNow, nodeMid, skewMillis);
    }

    /**
     * @param databaseNow the database's clock at the moment it answered
     * @param nodeNow     this node's estimate of that same moment (middle of the round trip)
     * @param skewMillis  {@code nodeNow - databaseNow}; positive means the node clock runs ahead
     */
    public record Measurement(LocalDateTime databaseNow, LocalDateTime nodeNow, long skewMillis) {

        public Duration skew() {
            return Duration.ofMillis(skewMillis);
        }

        public boolean exceeds(Duration limit) {
            return skew().abs().compareTo(limit.abs()) > 0;
        }
    }
}
