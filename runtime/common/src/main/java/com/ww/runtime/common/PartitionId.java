package com.ww.runtime.common;

/**
 * 分区标识。
 */
public record PartitionId(int value) {

    public PartitionId {
        if (value < 0) {
            throw new IllegalArgumentException("PartitionId.value must be >= 0, got " + value);
        }
    }

    public static PartitionId of(int value) {
        return new PartitionId(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
