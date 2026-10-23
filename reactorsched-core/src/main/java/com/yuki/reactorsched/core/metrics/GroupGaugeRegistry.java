package com.yuki.reactorsched.core.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps one gauge holder per (meter, executor group) pair so that a group's series keeps its last
 * value between updates instead of disappearing from a scrape.
 *
 * <p>A gauge registered with a plain double would freeze at the value it was registered with;
 * registering it against a mutable holder keeps it live. Two meters for the same group share the
 * group's holder only when they describe the same quantity, which is why the holder is keyed by
 * meter name as well.
 */
final class GroupGaugeRegistry {

    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, ReactorMetrics.Holder> holders = new ConcurrentHashMap<>();

    GroupGaugeRegistry(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * Registers (once) a gauge for {@code name} in {@code group} and returns its holder. The first
     * call seeds it with {@code initial}; later calls keep the current value. An empty group means
     * "cluster-wide" and gets no {@code executor_group} tag at all, so the series stays untagged
     * in Prometheus instead of carrying a meaningless empty label value.
     */
    ReactorMetrics.Holder gauge(String name, String group, double initial) {
        return holders.computeIfAbsent(key(name, group), ignored -> {
            ReactorMetrics.Holder holder = new ReactorMetrics.Holder();
            holder.set(initial);
            var builder = io.micrometer.core.instrument.Gauge.builder(name, holder, ReactorMetrics.Holder::value)
                    .description("ReactorSched gauge"
                            + (group.isEmpty() ? "" : " for executor group " + group));
            if (!group.isEmpty()) {
                builder.tags(Tags.of("executor_group", group));
            }
            builder.register(registry);
            return holder;
        });
    }

    private static String key(String name, String group) {
        return name + "|" + group;
    }
}
