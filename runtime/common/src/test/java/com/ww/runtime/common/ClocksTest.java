package com.ww.runtime.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClocksTest {

    @Test
    void testClockDefaultsToZero() {
        assertThat(new TestClock().nowMillis()).isZero();
    }

    @Test
    void testClockSetAndAdvance() {
        TestClock clock = new TestClock(100L);
        assertThat(clock.nowMillis()).isEqualTo(100L);

        clock.setMillis(500L);
        assertThat(clock.nowMillis()).isEqualTo(500L);

        clock.advance(50L);
        assertThat(clock.nowMillis()).isEqualTo(550L);

        clock.advance(-25L);
        assertThat(clock.nowMillis()).isEqualTo(525L);
    }

    @Test
    void systemClockAdvances() throws InterruptedException {
        SystemClock clock = SystemClock.instance();
        long before = clock.nowMillis();
        Thread.sleep(5L);
        assertThat(clock.nowMillis()).isGreaterThanOrEqualTo(before + 5L);
        assertThat(SystemClock.instance()).isSameAs(clock);
    }
}
