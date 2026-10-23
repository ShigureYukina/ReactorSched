package com.yuki.reactorsched.executor.registry.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * One registered executor process (table {@code executor_registry}, PRD 7.1).
 *
 * <p>The registry knows <em>who</em> may run tasks of a group and <em>whether</em> it is alive. It
 * knows nothing about the tasks themselves: assignment happens through the ordinary claim path,
 * whose {@code WHERE} clause enforces the group (PRD 7.2).
 */
@Data
@TableName("executor_registry")
public class Executor {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String appName;

    private String instanceId;

    private String groupName;

    private String address;

    private ExecutorStatus status;

    private LocalDateTime lastHeartbeatAt;

    private LocalDateTime registeredAt;

    /** Free-form JSON metadata the executor sent at registration. */
    private String metadata;
}
