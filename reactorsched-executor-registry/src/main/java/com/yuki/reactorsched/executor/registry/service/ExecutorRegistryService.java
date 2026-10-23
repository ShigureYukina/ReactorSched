package com.yuki.reactorsched.executor.registry.service;

import com.yuki.reactorsched.executor.registry.domain.Executor;
import com.yuki.reactorsched.executor.registry.domain.ExecutorStatus;
import com.yuki.reactorsched.executor.registry.dto.RegisterExecutorRequest;
import com.yuki.reactorsched.executor.registry.exception.ExecutorNotFoundException;
import com.yuki.reactorsched.executor.registry.exception.ExecutorOfflineException;
import com.yuki.reactorsched.executor.registry.mapper.ExecutorGroupStats;
import com.yuki.reactorsched.executor.registry.mapper.ExecutorRegistryMapper;
import com.yuki.reactorsched.executor.registry.properties.ExecutorProperties;
import com.yuki.reactorsched.core.metrics.ReactorMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Registration, heartbeat and liveness of executors (PRD 7.1).
 *
 * <p>Liveness is a heartbeat, not a connection: an executor that stops heartbeating is swept
 * OFFLINE and stops receiving tasks, while the instances it still holds stay protected by their
 * leases and are recovered through the ordinary lease path (PRD 8.3).
 */
@Profile("!executor")
@Service
public class ExecutorRegistryService {

    /** Longest accepted metadata blob; the column is JSON and nobody needs a megabyte of tags. */
    public static final int MAX_METADATA_LENGTH = 2048;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonNodeFactory JSON_FACTORY = JsonNodeFactory.instance;

    private final ExecutorRegistryMapper mapper;
    private final ExecutorProperties properties;
    private final Clock clock;

    public ExecutorRegistryService(ExecutorRegistryMapper mapper, ExecutorProperties properties, Clock clock) {
        this.mapper = mapper;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Registers an executor, or takes the row of the same {@code (app_name, instance_id)} back
     * online. The response is the stored row, resolved after the upsert - the upsert's return value
     * cannot tell insert from match (AGENTS.md 3).
     */
    @Transactional
    public Executor register(RegisterExecutorRequest request) {
        String appName = requireText(request.appName(), "appName");
        String instanceId = request.instanceId() == null || request.instanceId().isBlank()
                ? defaultInstanceId()
                : request.instanceId().trim();
        String metadata = normalizeMetadata(request.metadata(), request.maxConcurrency());
        LocalDateTime now = LocalDateTime.now(clock);
        mapper.register(appName, instanceId, request.effectiveGroup(), trimToNull(request.address()),
                metadata, now);
        Executor stored = mapper.findByIdentity(appName, instanceId);
        if (stored == null) {
            throw new IllegalStateException(
                    "executor " + appName + "/" + instanceId + " was not stored and cannot be read back");
        }
        return stored;
    }

    /**
     * Records one heartbeat.
     *
     * @return {@code false} when no such executor exists - the executor should re-register
     */
    @Transactional
    public boolean heartbeat(long executorId) {
        return mapper.heartbeat(executorId, LocalDateTime.now(clock)) == 1;
    }

    /**
     * The executor behind an id, which must exist and be ONLINE.
     *
     * @throws ExecutorNotFoundException  when the id is unknown
     * @throws ExecutorOfflineException   when the heartbeat went stale
     */
    public Executor requireOnline(long executorId) {
        Executor executor = requireExisting(executorId);
        if (executor.getStatus() != ExecutorStatus.ONLINE) {
            throw new ExecutorOfflineException(executorId);
        }
        return executor;
    }

    /**
     * The executor behind an id, which must exist but may be OFFLINE.
     *
     * <p>Result reporting and lease renewal go through this, not {@link #requireOnline}: an executor
     * that is alive but was just swept OFFLINE (a long GC pause, a clock hiccup) must still be able
     * to finish the attempts it holds. The lease, not the heartbeat, decides who owns a running
     * attempt (PRD 8.2).
     *
     * @throws ExecutorNotFoundException when the id is unknown
     */
    public Executor requireExisting(long executorId) {
        Executor executor = mapper.findById(executorId);
        if (executor == null) {
            throw new ExecutorNotFoundException(executorId);
        }
        return executor;
    }

    /** Online executors of one group, most recently heartbeating first. */
    public List<Executor> onlineExecutors(String groupName) {
        return mapper.findOnlineByGroup(groupName);
    }

    /** Every registered executor, for the open read API. */
    public List<Executor> list(ExecutorStatus status) {
        return status == null ? mapper.selectList(null) : mapper.findByStatus(status);
    }

    /**
     * Takes every executor whose heartbeat went stale offline (PRD 7.1).
     *
     * @return how many executors this call took offline
     */
    @Transactional
    public int sweepOffline() {
        LocalDateTime threshold = LocalDateTime.now(clock).minus(properties.effectiveOfflineThreshold());
        return mapper.markStaleOffline(threshold);
    }

    /**
     * Publishes the per-group executor gauges (PRD 8.6): online count and the age of the group's
     * freshest heartbeat. Computed once per sweep on the scheduling center rather than counted in
     * every process, so the series is the same wherever it is scraped.
     */
    public void publishExecutorGauges(ReactorMetrics metrics) {
        for (ExecutorGroupStats stats : mapper.groupStats(LocalDateTime.now(clock))) {
            metrics.declareGroup(stats.groupName());
            metrics.executorsOnline(stats.groupName(), stats.onlineCount());
            metrics.heartbeatAge(stats.groupName(), stats.heartbeatAge());
        }
    }

    /** Heartbeats older than this are stale: the sweep's threshold, exposed for tests. */
    public LocalDateTime offlineThreshold() {
        return LocalDateTime.now(clock).minus(properties.effectiveOfflineThreshold());
    }

    private static String defaultInstanceId() {
        String host;
        try {
            host = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        return host + ":" + ProcessHandle.current().pid();
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }

    private static String trimToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    /**
     * Normalizes the executor's metadata and records {@code maxConcurrency} inside it: the registry
     * table has no dedicated column for it (PRD 7.1 lists the columns without one), and dropping a
     * field the request explicitly carries would make {@code GET /executors} lie about the executor.
     * A metadata blob that is not a JSON object is kept verbatim - the executor sent something
     * non-standard and the field is informational anyway.
     */
    private static String normalizeMetadata(String metadata, Integer maxConcurrency) {
        String trimmed = metadata == null || metadata.isBlank() ? null : metadata.trim();
        if (maxConcurrency == null) {
            return bound(trimmed);
        }
        try {
            ObjectNode node = trimmed == null
                    ? JSON_FACTORY.objectNode()
                    : (ObjectNode) JSON.readTree(trimmed);
            node.put("maxConcurrency", maxConcurrency);
            return bound(node.toString());
        } catch (IOException | RuntimeException e) {
            return bound(trimmed);
        }
    }

    private static String bound(String metadata) {
        return metadata != null && metadata.length() > MAX_METADATA_LENGTH
                ? metadata.substring(0, MAX_METADATA_LENGTH)
                : metadata;
    }
}
