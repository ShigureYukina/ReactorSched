package com.yuki.reactorsched.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.JobStatus;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Persistence of job definitions.
 *
 * <p>Every state write is a conditional update: the {@code WHERE} clause carries the state the
 * caller believed it was acting on, so two schedulers, or an operator racing a scheduler, can never
 * both succeed on the same row. A return value of {@code 0} means "I lost the race", never "it
 * probably worked" (AGENTS.md 3, 4).
 */
public interface JobDefinitionMapper extends BaseMapper<JobDefinition> {

    @Select("""
            SELECT *
              FROM job_definition
             WHERE job_key = #{jobKey}
            """)
    JobDefinition findByKey(@Param("jobKey") String jobKey);

    /**
     * Definitions whose planned fire time has arrived, oldest first. Paused and deleted definitions
     * never have a {@code next_fire_at}, so the status filter is a second safety net rather than the
     * only one.
     */
    @Select("""
            SELECT *
              FROM job_definition
             WHERE status = 'ENABLED'
               AND next_fire_at IS NOT NULL
               AND next_fire_at <= #{now}
             ORDER BY next_fire_at ASC, id ASC
             LIMIT #{limit}
            """)
    List<JobDefinition> findDue(@Param("now") LocalDateTime now, @Param("limit") int limit);

    /**
     * Claims one fire time for the caller (PRD 6.2).
     *
     * <p>{@code next_fire_at} is both the value read by the scanner and the value compared in the
     * {@code WHERE} clause: if another scheduler advanced it in the meantime, this update affects no
     * rows and the caller skips the definition instead of creating duplicate instances.
     *
     * @return 1 when this caller won the fire time, 0 when another scheduler or an operator won
     */
    @Update("""
            UPDATE job_definition
               SET next_fire_at = #{nextFireAt},
                   last_fire_at = #{plannedFireAt},
                   updated_at = #{now}
             WHERE id = #{jobId}
               AND status = 'ENABLED'
               AND next_fire_at = #{observedNextFireAt}
            """)
    int claimFireTime(@Param("jobId") long jobId,
                      @Param("observedNextFireAt") LocalDateTime observedNextFireAt,
                      @Param("nextFireAt") LocalDateTime nextFireAt,
                      @Param("plannedFireAt") LocalDateTime plannedFireAt,
                      @Param("now") LocalDateTime now);

    /**
     * Claims the last fire time of a schedule (a one-off, or a cron that has no further occurrence):
     * the fire time is recorded and the definition is parked in {@code PAUSED} with no next fire
     * time, so the scanner stops seeing it (PRD 6.1).
     *
     * @return 1 when this caller won the fire time, 0 when another scheduler or an operator won
     */
    @Update("""
            UPDATE job_definition
               SET status = 'PAUSED',
                   next_fire_at = NULL,
                   last_fire_at = #{plannedFireAt},
                   updated_at = #{now}
             WHERE id = #{jobId}
               AND status = 'ENABLED'
               AND next_fire_at = #{observedNextFireAt}
            """)
    int claimFinalFireTime(@Param("jobId") long jobId,
                           @Param("observedNextFireAt") LocalDateTime observedNextFireAt,
                           @Param("plannedFireAt") LocalDateTime plannedFireAt,
                           @Param("now") LocalDateTime now);

    /**
     * Moves a definition past fire times that are not going to be executed (a misfire under
     * {@code SKIP}, or the part of a catch-up beyond its limit). Nothing is fired, so
     * {@code last_fire_at} - the fire time that was turned into instances - stays untouched.
     *
     * @return 1 when this caller won the advance, 0 when another scheduler or an operator won
     */
    @Update("""
            UPDATE job_definition
               SET next_fire_at = #{nextFireAt},
                   updated_at = #{now}
             WHERE id = #{jobId}
               AND status = 'ENABLED'
               AND next_fire_at = #{observedNextFireAt}
            """)
    int skipToNextFireTime(@Param("jobId") long jobId,
                           @Param("observedNextFireAt") LocalDateTime observedNextFireAt,
                           @Param("nextFireAt") LocalDateTime nextFireAt,
                           @Param("now") LocalDateTime now);

    /**
     * Parks a definition that will never fire again: a one-off whose time passed under a skipping
     * misfire policy, or a schedule that ran out of occurrences. Nothing is fired and no fire time
     * is recorded.
     *
     * @return 1 when this caller won the retire, 0 when another scheduler or an operator won
     */
    @Update("""
            UPDATE job_definition
               SET status = 'PAUSED',
                   next_fire_at = NULL,
                   updated_at = #{now}
             WHERE id = #{jobId}
               AND status = 'ENABLED'
               AND next_fire_at = #{observedNextFireAt}
            """)
    int retireDefinition(@Param("jobId") long jobId,
                         @Param("observedNextFireAt") LocalDateTime observedNextFireAt,
                         @Param("now") LocalDateTime now);

    /**
     * The database's own clock, used to detect drift between the scheduler nodes and the single
     * source of task state (PRD 6.5). A node whose clock has drifted decides about fire times with
     * a wrong "now", which is a correctness problem rather than a monitoring one.
     */
    @Select("SELECT NOW(3)")
    LocalDateTime databaseNow();

    /**
     * Pauses or resumes a definition, resetting the planned fire time in the same statement so the
     * two can never disagree. A paused definition must not keep a {@code next_fire_at}: the scanner
     * would happily pick it up otherwise (PRD 4.2).
     */
    @Update("""
            UPDATE job_definition
               SET status = #{toStatus},
                   next_fire_at = #{nextFireAt},
                   updated_by = #{operator},
                   updated_at = #{now}
             WHERE id = #{jobId}
               AND status = #{fromStatus}
            """)
    int changeStatus(@Param("jobId") long jobId,
                     @Param("fromStatus") JobStatus fromStatus,
                     @Param("toStatus") JobStatus toStatus,
                     @Param("nextFireAt") LocalDateTime nextFireAt,
                     @Param("operator") String operator,
                     @Param("now") LocalDateTime now);

    /** Soft delete: the row stays for the instances that still point at it (PRD 4.2). */
    @Update("""
            UPDATE job_definition
               SET status = 'DELETED',
                   next_fire_at = NULL,
                   deleted_at = #{now},
                   updated_by = #{operator},
                   updated_at = #{now}
             WHERE id = #{jobId}
               AND status IN ('ENABLED', 'PAUSED')
            """)
    int softDelete(@Param("jobId") long jobId,
                   @Param("operator") String operator,
                   @Param("now") LocalDateTime now);

    /**
     * Moves a paused or enabled definition to the next planned time after an edit that changed how it
     * is scheduled. The observed value is part of the {@code WHERE} clause, so an edit never races the
     * scheduler into a lost or duplicated fire time.
     */
    @Update("""
            UPDATE job_definition
               SET next_fire_at = #{nextFireAt},
                   updated_by = #{operator},
                   updated_at = #{now}
             WHERE id = #{jobId}
               AND status = #{status}
               AND next_fire_at <=> #{observedNextFireAt}
            """)
    int reschedule(@Param("jobId") long jobId,
                   @Param("status") JobStatus status,
                   @Param("observedNextFireAt") LocalDateTime observedNextFireAt,
                   @Param("nextFireAt") LocalDateTime nextFireAt,
                   @Param("operator") String operator,
                   @Param("now") LocalDateTime now);

    @Select("""
            <script>
            SELECT COUNT(*)
              FROM job_definition
            <where>
                <if test="status != null">AND status = #{status}</if>
                <if test="from != null">AND created_at &gt;= #{from}</if>
                <if test="to != null">AND created_at &lt; #{to}</if>
            </where>
            </script>
            """)
    long countByQuery(@Param("status") JobStatus status,
                      @Param("from") LocalDateTime from,
                      @Param("to") LocalDateTime to);

    @Select("""
            <script>
            SELECT *
              FROM job_definition
            <where>
                <if test="status != null">AND status = #{status}</if>
                <if test="from != null">AND created_at &gt;= #{from}</if>
                <if test="to != null">AND created_at &lt; #{to}</if>
            </where>
             ORDER BY created_at DESC, id DESC
             LIMIT #{limit} OFFSET #{offset}
            </script>
            """)
    List<JobDefinition> findPage(@Param("status") JobStatus status,
                                 @Param("from") LocalDateTime from,
                                 @Param("to") LocalDateTime to,
                                 @Param("limit") int limit,
                                 @Param("offset") long offset);
}
