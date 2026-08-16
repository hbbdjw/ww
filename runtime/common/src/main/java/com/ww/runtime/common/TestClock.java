package com.ww.runtime.common;

/**
 * 可控测试时钟：支持设置与推进。
 */
public final class TestClock implements Clock {

    private long millis;

    public TestClock(long initialMillis) {
        this.millis = initialMillis;
    }

    public TestClock() {
        this(0L);
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

    public void advance(long deltaMillis) {
        this.millis += deltaMillis;
    }

    @Override
    public long nowMillis() {
        return millis;
    }
}
