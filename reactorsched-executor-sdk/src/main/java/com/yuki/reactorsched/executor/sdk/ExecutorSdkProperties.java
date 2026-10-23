package com.yuki.reactorsched.executor.sdk;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Configuration of one executor process (prefix {@code reactorsched.executor}, PRD 7).
 *
 * <p>An executor process configures only its own side of the contract: where the scheduling center
 * is, which group it serves, how much it may run at once and how its bounded pool behaves. The
 * center's own {@code reactorsched.executor.*} properties (token check, offline sweep, lease ttl)
 * live in the scheduling center and are deliberately not duplicated here.
 */
@ConfigurationProperties("reactorsched.executor")
public class ExecutorSdkProperties {

    /** Base URL of the scheduling center, e.g. {@code http://scheduler:8080}. */
    private String baseUrl = "";

    /** Shared secret sent as {@code X-ReactorSched-Token} on every call (PRD 10.3). */
    private String token = "";

    /** Application name this executor registers under. */
    private String appName = "reactorsched-executor";

    /** Instance identity; defaults to {@code host:pid} so a restart takes its row back (PRD 7.1). */
    private String instanceId = "";

    /** Executor group this process serves; must match {@code task.executor_group} (PRD 7.2). */
    private String group = "DEFAULT";

    /** How many attempts this process may run at the same time (PRD 7.4). */
    private int maxConcurrency = 4;

    /** Advertised address, informational only. */
    private String address = "";

    /** Free-form JSON metadata sent at registration, e.g. version or tags. */
    private String metadata = "";

    /** How often the executor proves it is alive (PRD 7.1). */
    private Duration heartbeatInterval = Duration.ofSeconds(10);

    /** How often the executor asks for work; claiming is a pull, never a push (PRD 7.2). */
    private Duration pollInterval = Duration.ofMillis(500);

    /** Upper bound of one claim request; the center caps it as well (PRD 7.2). */
    private int batchSize = 10;

    /** Capacity of the bounded execution queue; an unbounded queue is forbidden (PRD 7.4). */
    private int queueCapacity = 256;

    /**
     * How often a running attempt's lease is renewed. It must stay below the center's lease ttl,
     * otherwise the attempt would be reclaimed while it is still running (PRD 7.4, 8.2).
     */
    private Duration leaseRenewInterval = Duration.ofSeconds(20);

    /** How long a graceful shutdown waits for running attempts before interrupting them (PRD 7.4). */
    private Duration shutdownGrace = Duration.ofSeconds(30);

    /** Per task type concurrency caps, applied on top of {@link #maxConcurrency} (PRD 7.4). */
    private Map<String, Integer> taskTypeLimits = new LinkedHashMap<>();

    /** Timeout of one call to the scheduling center, so a hung round cannot stall the loops. */
    private Duration requestTimeout = Duration.ofSeconds(10);

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public Duration getLeaseRenewInterval() {
        return leaseRenewInterval;
    }

    public void setLeaseRenewInterval(Duration leaseRenewInterval) {
        this.leaseRenewInterval = leaseRenewInterval;
    }

    public Duration getShutdownGrace() {
        return shutdownGrace;
    }

    public void setShutdownGrace(Duration shutdownGrace) {
        this.shutdownGrace = shutdownGrace;
    }

    public Map<String, Integer> getTaskTypeLimits() {
        return taskTypeLimits;
    }

    public void setTaskTypeLimits(Map<String, Integer> taskTypeLimits) {
        this.taskTypeLimits = taskTypeLimits == null ? new LinkedHashMap<>() : taskTypeLimits;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    /** Effective group, applying the default when the configuration left it blank. */
    public String effectiveGroup() {
        return group == null || group.isBlank() ? "DEFAULT" : group.trim();
    }

    /** Effective instance identity: the configured one, or {@code host:pid}. */
    public String effectiveInstanceId() {
        if (instanceId != null && !instanceId.isBlank()) {
            return instanceId.trim();
        }
        String host;
        try {
            host = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        return host + ":" + ProcessHandle.current().pid();
    }

    /**
     * Concurrency cap of one task type, or 0 when the type is not capped. Keys are compared
     * case-insensitively, like the handler registry compares task types.
     */
    public int limitFor(String taskType) {
        if (taskType == null) {
            return 0;
        }
        String normalized = taskType.trim().toLowerCase(Locale.ROOT);
        for (Map.Entry<String, Integer> entry : taskTypeLimits.entrySet()) {
            if (entry.getKey() != null && entry.getKey().trim().toLowerCase(Locale.ROOT).equals(normalized)) {
                Integer limit = entry.getValue();
                return limit == null ? 0 : limit;
            }
        }
        return 0;
    }
}
