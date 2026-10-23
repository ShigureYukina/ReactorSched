package com.yuki.reactorsched.core.concurrency;

import com.yuki.reactorsched.core.mapper.GroupRateWindowMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The store is the boundary between the adaptive controller (writes) and every executor of the
 * group (reads); the conditional-update guarantees themselves live in {@code group_rate_window} and
 * are covered by {@code GroupRateLimitIT}.
 */
@ExtendWith(MockitoExtension.class)
class GroupConcurrencyStoreTest {

    @Mock
    private GroupRateWindowMapper mapper;

    @Test
    void writesPublishTheLimitOnTheStateRow() {
        new GroupConcurrencyStore(mapper).write("etl", 6, LocalDateTime.parse("2026-10-22T10:00:00"));

        verify(mapper).upsertGroupLimit("etl", 6, LocalDateTime.parse("2026-10-22T10:00:00"));
    }

    @Test
    void readsReturnThePublishedLimit() {
        when(mapper.findGroupLimit("etl")).thenReturn(6);

        assertThat(new GroupConcurrencyStore(mapper).read("etl")).isEqualTo(6);
    }

    @Test
    void aGroupWithoutAPublishedRowReadsAsNoLimit() {
        when(mapper.findGroupLimit("fresh")).thenReturn(null);

        assertThat(new GroupConcurrencyStore(mapper).read("fresh"))
                .isEqualTo(GroupConcurrencyStore.NO_LIMIT);
    }

    @Test
    void readingNeverWrites() {
        when(mapper.findGroupLimit(anyString())).thenReturn(null);

        new GroupConcurrencyStore(mapper).read("fresh");

        verify(mapper, never()).upsertGroupLimit(anyString(), anyInt(), any(LocalDateTime.class));
        assertThat(GroupConcurrencyStore.NO_LIMIT).isEqualTo(-1);
    }
}
