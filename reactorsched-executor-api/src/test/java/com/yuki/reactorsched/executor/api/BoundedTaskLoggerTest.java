package com.yuki.reactorsched.executor.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The bounded log buffer is the executor's memory guard (PRD 9): a handler that logs without end
 * must neither grow the heap nor push an unbounded blob at the scheduling center. The tail of an
 * attempt survives because it is the part that explains a failure.
 */
class BoundedTaskLoggerTest {

    @Test
    void keepsEverythingThatFits() {
        BoundedTaskLogger logger = new BoundedTaskLogger(1024);

        logger.log("first");
        logger.log("second");

        assertThat(logger.text()).isEqualTo("first\nsecond\n");
        assertThat(logger.truncated()).isFalse();
    }

    @Test
    void dropsTheOldestLinesWhenTheBufferIsFull() {
        BoundedTaskLogger logger = new BoundedTaskLogger(20);

        logger.log("aaaa");   // 5 bytes
        logger.log("bbbb");   // 10
        logger.log("cccc");   // 15
        logger.log("dddd");   // 20
        logger.log("eeee");   // would be 25: the oldest lines go first

        assertThat(logger.text()).isEqualTo("bbbb\ncccc\ndddd\neeee\n");
        assertThat(logger.truncated()).isTrue();
    }

    @Test
    void keepsTheHeadOfALineLargerThanTheWholeBuffer() {
        BoundedTaskLogger logger = new BoundedTaskLogger(8);

        logger.log("0123456789abcdef");

        // The cut line keeps its head; the trailing newline is the first byte that goes.
        assertThat(logger.text()).isEqualTo("01234567");
        assertThat(logger.truncated()).isTrue();
    }

    @Test
    void countsMultibyteCharactersAsBytesNotCharacters() {
        // 4 CJK characters are 12 UTF-8 bytes but only 4 Java chars.
        BoundedTaskLogger logger = new BoundedTaskLogger(10);

        logger.log("中文");
        logger.log("中文");

        assertThat(logger.text()).isEqualTo("中文\n");
        assertThat(logger.truncated()).isTrue();
    }

    @Test
    void ignoresEmptyAndNullLines() {
        BoundedTaskLogger logger = new BoundedTaskLogger(64);

        logger.log(null);
        logger.log("");
        logger.log("kept");

        assertThat(logger.text()).isEqualTo("kept\n");
        assertThat(logger.truncated()).isFalse();
    }

    @Test
    void rejectsANonPositiveBuffer() {
        assertThatThrownBy(() -> new BoundedTaskLogger(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
