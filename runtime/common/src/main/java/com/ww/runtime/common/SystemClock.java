package com.ww.runtime.common;

/**
 * 系统时钟。
 */
public final class SystemClock implements Clock {

    private static final SystemClock INSTANCE = new SystemClock();

    private SystemClock() {
    }

    public static SystemClock instance() {
        return INSTANCE;
    }

    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
