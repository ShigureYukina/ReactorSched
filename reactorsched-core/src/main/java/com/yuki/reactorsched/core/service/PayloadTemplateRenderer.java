package com.yuki.reactorsched.core.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yuki.reactorsched.core.domain.JobDefinition;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Renders a job's payload template into the payload an instance carries (PRD 6.3).
 *
 * <p>The result is a snapshot: once an instance exists, editing the template has no effect on it, so
 * a reader can always answer "what exactly was this instance asked to do?".
 *
 * <p>Supported placeholders, replaced anywhere in a string value of the template:
 * <ul>
 *     <li>{@code ${jobKey}} - the definition's business key</li>
 *     <li>{@code ${jobId}} - the definition's numeric id</li>
 *     <li>{@code ${shardIndex}} - zero based shard number of this instance</li>
 *     <li>{@code ${shardTotal}} - number of shards the fire time was split into</li>
 *     <li>{@code ${scheduledAt}} - planned fire time, ISO-8601 in UTC</li>
 *     <li>{@code ${fireTimeEpochMs}} - planned fire time in epoch milliseconds, handy for idempotency keys</li>
 * </ul>
 * Unknown placeholders are left untouched rather than silently dropped: a template typo should be
 * visible in the payload the handler receives.
 */
@Profile("!executor")
@Component
public class PayloadTemplateRenderer {

    private final ObjectMapper objectMapper;

    public PayloadTemplateRenderer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * @throws IllegalArgumentException when the template is not a JSON object
     */
    public String render(JobDefinition job, int shardIndex, int shardTotal, LocalDateTime scheduledAt) {
        JsonNode template;
        try {
            template = objectMapper.readTree(job.getPayloadTemplate() == null ? "{}" : job.getPayloadTemplate());
        } catch (Exception e) {
            throw new IllegalArgumentException("payloadTemplate is not valid JSON: " + e.getMessage(), e);
        }
        if (template == null || !template.isObject()) {
            throw new IllegalArgumentException("payloadTemplate must be a JSON object");
        }
        ObjectNode rendered = ((ObjectNode) template).deepCopy();
        replaceInPlace(rendered, placeholders(job, shardIndex, shardTotal, scheduledAt));
        // The shard coordinates are always present, so a handler does not have to parse them out of
        // free text and cannot be surprised by a template that forgot one of the placeholders.
        rendered.put("_shardIndex", shardIndex);
        rendered.put("_shardTotal", shardTotal);
        rendered.put("_jobId", job.getId());
        rendered.put("_jobKey", job.getJobKey());
        return rendered.toString();
    }

    private java.util.Map<String, String> placeholders(JobDefinition job, int shardIndex, int shardTotal,
                                                       LocalDateTime scheduledAt) {
        String scheduled = scheduledAt == null ? "" : scheduledAt.toInstant(ZoneOffset.UTC).toString();
        long epochMs = scheduledAt == null ? 0L : Duration.between(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC), scheduledAt).toMillis();
        return java.util.Map.of(
                "${jobKey}", job.getJobKey() == null ? "" : job.getJobKey(),
                "${jobId}", String.valueOf(job.getId()),
                "${shardIndex}", String.valueOf(shardIndex),
                "${shardTotal}", String.valueOf(shardTotal),
                "${scheduledAt}", scheduled,
                "${fireTimeEpochMs}", String.valueOf(epochMs));
    }

    /** Replaces placeholders inside string values and object keys, leaving numbers and booleans alone. */
    private void replaceInPlace(JsonNode node, java.util.Map<String, String> placeholders) {
        if (node == null) {
            return;
        }
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            java.util.List<String> fieldNames = new java.util.ArrayList<>();
            object.fieldNames().forEachRemaining(fieldNames::add);
            for (String name : fieldNames) {
                JsonNode value = object.get(name);
                String replacedName = replace(name, placeholders);
                JsonNode replacedValue;
                if (value.isTextual()) {
                    replacedValue = object.textNode(replace(value.asText(), placeholders));
                } else {
                    replaceInPlace(value, placeholders);
                    replacedValue = value;
                }
                if (!replacedName.equals(name)) {
                    object.remove(name);
                }
                object.set(replacedName, replacedValue);
            }
        } else if (node.isArray()) {
            for (JsonNode element : node) {
                replaceInPlace(element, placeholders);
            }
        }
    }

    private static String replace(String text, java.util.Map<String, String> placeholders) {
        String result = text;
        for (java.util.Map.Entry<String, String> entry : placeholders.entrySet()) {
            result = result.replace(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
