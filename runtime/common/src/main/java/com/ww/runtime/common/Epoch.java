package com.ww.runtime.common;

/**
 * 单调纪元：用于防止过期消息/过期领导者写入。
 */
public record Epoch(long value) {

    public Epoch {
        if (value < 0) {
            throw new IllegalArgumentException("Epoch.value must be >= 0, got " + value);
        }
    }

    public static Epoch of(long value) {
        return new Epoch(value);
    }

    public boolean isStalerThan(Epoch other) {
        return this.value < other.value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
