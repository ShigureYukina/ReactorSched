package com.yuki.reactorsched.executor.registry.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactorsched.executor.registry.domain.Executor;
import com.yuki.reactorsched.executor.registry.domain.ExecutorStatus;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Persistence of the executor registry.
 *
 * <p>Registration is an idempotent upsert on {@code (app_name, instance_id)}: a restarted executor
 * process takes its row back instead of creating a second one. Whether the upsert inserted or
 * matched cannot be read from its return value (the driver counts matched rows,
 * {@code CLIENT_FOUND_ROWS}), so callers resolve the row afterwards - the same discipline as
 * instance creation (AGENTS.md 3).
 */
public interface ExecutorRegistryMapper extends BaseMapper<Executor> {

    /**
     * Registers an executor, or takes an existing row of the same {@code (app_name, instance_id)}
     * back online. {@code registered_at} keeps the first registration; everything else describes the
     * process that is alive right now.
     */
    @Insert("""
            INSERT INTO executor_registry (app_name, instance_id, group_name, address, status,
                last_heartbeat_at, registered_at, metadata)
            VALUES (#{appName}, #{instanceId}, #{groupName}, #{address}, 'ONLINE', #{now}, #{now}, #{metadata})
            ON DUPLICATE KEY UPDATE group_name = VALUES(group_name),
                                    address = VALUES(address),
                                    status = 'ONLINE',
                                    last_heartbeat_at = VALUES(last_heartbeat_at),
                                    metadata = VALUES(metadata)
            """)
    int register(@Param("appName") String appName,
                 @Param("instanceId") String instanceId,
                 @Param("groupName") String groupName,
                 @Param("address") String address,
                 @Param("metadata") String metadata,
                 @Param("now") LocalDateTime now);

    @Select("""
            SELECT *
              FROM executor_registry
             WHERE app_name = #{appName}
               AND instance_id = #{instanceId}
            """)
    Executor findByIdentity(@Param("appName") String appName, @Param("instanceId") String instanceId);

    @Select("SELECT * FROM executor_registry WHERE id = #{id}")
    Executor findById(@Param("id") long id);

    /**
     * Records one heartbeat. An executor that had been swept OFFLINE comes back ONLINE here: it
     * proved it is alive. The executor id in the path is the identity - the shared executor token
     * authenticates the fleet, not one process against another (PRD 10.3).
     *
     * @return 1 when the heartbeat was recorded, 0 when no such executor exists
     */
    @Update("""
            UPDATE executor_registry
               SET status = 'ONLINE',
                   last_heartbeat_at = #{now}
             WHERE id = #{id}
            """)
    int heartbeat(@Param("id") long id,
                  @Param("now") LocalDateTime now);

    /** Online executors of one group, most recently heartbeating first. */
    @Select("""
            SELECT *
              FROM executor_registry
             WHERE status = 'ONLINE'
               AND group_name = #{groupName}
             ORDER BY last_heartbeat_at DESC, id ASC
            """)
    List<Executor> findOnlineByGroup(@Param("groupName") String groupName);

    /**
     * One row per executor group: how many of its executors are ONLINE and how old the group's
     * freshest heartbeat is. The gauges of PRD 8.6 publish these, computed once per sweep on the
     * scheduling center rather than counted in every process.
     */
    @Select("""
            SELECT group_name AS groupName,
                   SUM(CASE WHEN status = 'ONLINE' THEN 1 ELSE 0 END) AS onlineCount,
                   TIMESTAMPDIFF(SECOND, MAX(last_heartbeat_at), #{now}) AS heartbeatAgeSeconds
              FROM executor_registry
             GROUP BY group_name
            """)
    List<ExecutorGroupStats> groupStats(@Param("now") LocalDateTime now);

    @Select("""
            SELECT *
              FROM executor_registry
             WHERE status = #{status}
             ORDER BY group_name ASC, id ASC
            """)
    List<Executor> findByStatus(@Param("status") ExecutorStatus status);

    /**
     * Online executors whose last heartbeat is older than the threshold: the candidates of the
     * offline sweep (PRD 7.1).
     */
    @Select("""
            SELECT *
              FROM executor_registry
             WHERE status = 'ONLINE'
               AND (last_heartbeat_at IS NULL OR last_heartbeat_at < #{threshold})
             ORDER BY id ASC
            """)
    List<Executor> findStale(@Param("threshold") LocalDateTime threshold);

    /**
     * Takes every stale online executor offline in one statement. The sweep runs on several
     * scheduling-center nodes; each row transitions exactly once because the {@code WHERE} clause
     * carries the state the sweep believed it was acting on.
     *
     * @return the number of executors this call took offline
     */
    @Update("""
            UPDATE executor_registry
               SET status = 'OFFLINE'
             WHERE status = 'ONLINE'
               AND (last_heartbeat_at IS NULL OR last_heartbeat_at < #{threshold})
            """)
    int markStaleOffline(@Param("threshold") LocalDateTime threshold);
}
