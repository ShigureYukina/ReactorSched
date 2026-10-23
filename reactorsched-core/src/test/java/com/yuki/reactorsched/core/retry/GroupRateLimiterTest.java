package com.yuki.reactorsched.core.retry;

import com.yuki.reactorsched.core.mapper.GroupRateWindowMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The limiter itself only orchestrates the three SQL statements; the cross-process guarantee comes
 * from the conditional update inside {@code group_rate_window} and is covered by
 * {@code GroupRateLimitIT} against real MySQL.
 */
@ExtendWith(MockitoExtension.class)
class GroupRateLimiterTest {

    @Mock
    private GroupRateWindowMapper mapper;

    private Clock clock;

    private static final long WINDOW = Instant.parse("2026-10-22T10:00:00Z").getEpochSecond();

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(Instant.parse("2026-10-22T10:00:00Z"), ZoneOffset.UTC);
    }

    private GroupRateLimiter limiter(int maxPerSecond) {
        RetryProperties properties = new RetryProperties();
        properties.setMaxDispatchesPerSecond(maxPerSecond);
        return new GroupRateLimiter(mapper, "DEFAULT", properties, clock);
    }

    @Test
    void disabledLimitNeverTouchesTheDatabase() {
        assertThat(limiter(0).tryAcquire()).isTrue();
        verify(mapper, never()).openWindow(anyString(), anyLong());
        verify(mapper, never()).tryAdmit(anyString(), anyLong(), anyInt());
    }

    @Test
    void admitsWhenTheConditionalUpdateWinsTheRow() {
        when(mapper.tryAdmit(eq("DEFAULT"), eq(WINDOW), eq(20))).thenReturn(1);

        assertThat(limiter(20).tryAcquire()).isTrue();

        verify(mapper).openWindow("DEFAULT", WINDOW);
        verify(mapper).pruneWindows("DEFAULT", WINDOW - 120L);
    }

    @Test
    void defersWhenAnotherExecutorExhaustedTheWindow() {
        when(mapper.tryAdmit(eq("DEFAULT"), eq(WINDOW), eq(20))).thenReturn(0);

        assertThat(limiter(20).tryAcquire()).isFalse();
    }

    @Test
    void prunesOnlyThisGroupsExpiredWindows() {
        when(mapper.tryAdmit(anyString(), anyLong(), anyInt())).thenReturn(1);

        limiter(5).tryAcquire();

        ArgumentCaptor<Long> before = ArgumentCaptor.forClass(Long.class);
        verify(mapper).pruneWindows(eq("DEFAULT"), before.capture());
        assertThat(before.getValue()).isEqualTo(WINDOW - 120L);
    }

    @Test
    void groupFromConfigurationIsCarriedIntoEveryStatement() {
        RetryProperties properties = new RetryProperties();
        properties.setMaxDispatchesPerSecond(3);
        GroupRateLimiter limiter = new GroupRateLimiter(mapper, "etl", properties, clock);
        when(mapper.tryAdmit(eq("etl"), anyLong(), eq(3))).thenReturn(1);

        assertThat(limiter.executorGroup()).isEqualTo("etl");
        assertThat(limiter.maxPerSecond()).isEqualTo(3);
        assertThat(limiter.tryAcquire()).isTrue();
        verify(mapper).openWindow(eq("etl"), anyLong());
    }
}
