package com.yuki.reactorsched.executor.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The handler registry is the executor's front door: a duplicate or blank task type must fail the
 * process at startup instead of silently shadowing a handler, and an unknown type must be rejected
 * loudly instead of returning {@code null} into an execution path.
 */
class TaskHandlerRegistryTest {

    @Test
    void resolvesHandlersCaseInsensitively() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(handler("Echo")));

        assertThat(registry.knows("echo")).isTrue();
        assertThat(registry.knows("ECHO")).isTrue();
        assertThat(registry.registeredTypes()).containsExactly("echo");
        assertThat(registry.require("echo").taskType()).isEqualTo("Echo");
    }

    @Test
    void rejectsDuplicateTaskTypesAtStartup() {
        assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(handler("echo"), handler("ECHO"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate task type");
    }

    @Test
    void rejectsBlankTaskTypesAtStartup() {
        assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(handler("  "))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("blank task type");
    }

    @Test
    void unknownTaskTypeIsRejectedInsteadOfReturningNull() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of(handler("echo")));

        assertThat(registry.knows("other")).isFalse();
        assertThatThrownBy(() -> registry.require("other"))
                .isInstanceOf(UnknownTaskTypeException.class)
                .hasMessageContaining("other");
    }

    @Test
    void emptyRegistryIsUsableButKnowsNothing() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry(List.of());

        assertThat(registry.registeredTypes()).isEmpty();
        assertThat(registry.knows("echo")).isFalse();
    }

    @Test
    void contextCarriesTaskIdentityAndPayload() throws Exception {
        JsonNode payload = new ObjectMapper().readTree("{\"k\":\"v\"}");
        TaskContext context = new TaskContext(42L, 2, "echo", payload, 1, 3, DiscardingTaskLogger.instance());

        assertThat(context.taskId()).isEqualTo(42L);
        assertThat(context.attempt()).isEqualTo(2);
        assertThat(context.taskType()).isEqualTo("echo");
        assertThat(context.payload()).isEqualTo(payload);
        assertThat(context.shardIndex()).isEqualTo(1);
        assertThat(context.shardTotal()).isEqualTo(3);
        // The business idempotency key is stable across retries of the same shard (PRD 8.1).
        assertThat(context.idempotencyKey()).isEqualTo("42:2:1");
    }

    private static TaskHandler handler(String taskType) {
        return new TaskHandler() {
            @Override
            public String taskType() {
                return taskType;
            }

            @Override
            public void execute(TaskContext context) {
                // Not executed: the registry tests only resolve handlers.
            }
        };
    }
}
