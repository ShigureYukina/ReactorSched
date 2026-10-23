package com.yuki.reactorsched.core.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * Shared group state (PRD 12.3): retry dispatch rate and the AIMD concurrency limit, both visible
 * across executor processes. See {@code V11__group_rate_window.sql} for the row layout - rate
 * window rows carry {@code window_second > 0}, the permanent per-group concurrency state row is
 * {@code window_second = 0}.
 *
 * <p>Admission is decided by a conditional update: {@code tryAdmit} affects a row only while the
 * window is below its cap, so concurrent executors of the same group are throttled by MySQL row
 * contention, not by any process-local counter.
 */
public interface GroupRateWindowMapper {

    /** Creates the rate window row if a sibling executor has not opened it yet; never fails on duplicates. */
    @Insert("INSERT IGNORE INTO group_rate_window (executor_group, window_second, admitted) "
            + "VALUES (#{group}, #{windowSecond}, 0)")
    void openWindow(@Param("group") String group, @Param("windowSecond") long windowSecond);

    /**
     * Atomically admits one retry dispatch into the window. Returns 1 when the window still had
     * capacity ({@code admitted < maxPerSecond}) and 0 when the cap was already reached - the
     * caller treats 0 as "defer the retry to the next poll".
     */
    @Update("UPDATE group_rate_window SET admitted = admitted + 1 "
            + "WHERE executor_group = #{group} AND window_second = #{windowSecond} AND admitted < #{maxPerSecond}")
    int tryAdmit(@Param("group") String group, @Param("windowSecond") long windowSecond,
                 @Param("maxPerSecond") int maxPerSecond);

    /** Deletes a group's expired rate window rows, keeping the table small between retention runs. */
    @Delete("DELETE FROM group_rate_window "
            + "WHERE executor_group = #{group} AND window_second > 0 AND window_second < #{beforeSecond}")
    int pruneWindows(@Param("group") String group, @Param("beforeSecond") long beforeSecond);

    /**
     * Publishes the group's shared AIMD concurrency on the permanent state row
     * ({@code window_second = 0}). The row has no auto-increment key and the return value is never
     * used for branching, so the {@code CLIENT_FOUND_ROWS} pitfall does not apply here.
     */
    @Insert("INSERT INTO group_rate_window (executor_group, window_second, admitted, current_limit, updated_at) "
            + "VALUES (#{group}, 0, 0, #{currentLimit}, #{updatedAt}) "
            + "ON DUPLICATE KEY UPDATE current_limit = VALUES(current_limit), updated_at = VALUES(updated_at)")
    void upsertGroupLimit(@Param("group") String group, @Param("currentLimit") int currentLimit,
                          @Param("updatedAt") java.time.LocalDateTime updatedAt);

    /** The shared concurrency of a group; {@code null} when the group has never been adjusted. */
    @Select("SELECT current_limit FROM group_rate_window "
            + "WHERE executor_group = #{group} AND window_second = 0")
    Integer findGroupLimit(@Param("group") String group);
}
