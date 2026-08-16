package com.ww.runtime.common;

/**
 * 通用版本号：单调递增的修改序号。
 */
public record Version(long value) {

    public Version {
        if (value < 0) {
            throw new IllegalArgumentException("Version.value must be >= 0, got " + value);
        }
    }

    public static Version of(long value) {
        return new Version(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
