package com.ww.runtime.data.cache;

import java.util.Objects;

/**
 * CACHE 能力的分区内条目视图（迁移载体）：key + 剩余 TTL + 值。
 *
 * @param key                缓存键
 * @param remainingTtlMillis 剩余存活毫秒：{@code <=0} 永不过期，{@code >0} 剩余 TTL
 * @param value              缓存值（plain KV；容器类型不参与迁移，见 CacheCapability 约束）
 */
public record CachePartitionEntry(String key, long remainingTtlMillis, Object value) {

    public CachePartitionEntry {
        Objects.requireNonNull(key, "key");
    }
}
