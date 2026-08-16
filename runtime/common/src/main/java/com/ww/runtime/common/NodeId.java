package com.ww.runtime.common;

/**
 * 节点唯一标识。
 */
public record NodeId(String value) {

    public NodeId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("NodeId.value must not be null or blank");
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
