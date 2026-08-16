package com.ww.runtime.common;

import java.util.Locale;

/**
 * 能力类型：MOCK（演示/fencing 参考）、CACHE（分布式缓存）、STORAGE（对象存储）。
 */
public enum CapabilityType {

    MOCK("mock"),
    CACHE("cache"),
    STORAGE("storage");

    private final String alias;

    CapabilityType(String alias) {
        this.alias = alias;
    }

    public String alias() {
        return alias;
    }

    public static CapabilityType parse(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("capability type text must not be null or blank");
        }
        String normalized = text.trim().toLowerCase(Locale.ROOT);
        for (CapabilityType type : values()) {
            if (type.name().toLowerCase(Locale.ROOT).equals(normalized) || type.alias.equals(normalized)) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown capability type: " + text);
    }

    public static CapabilityType of(String alias) {
        return parse(alias);
    }
}
