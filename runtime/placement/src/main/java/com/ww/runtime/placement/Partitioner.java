package com.ww.runtime.placement;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * key → partition 的静态映射函数。
 *
 * <p>与现行系统 hash 同族：与 {@link WeightedRendezvousHash} 一样取
 * MD5(input) 前 8 字节构造 64 位值、清除符号位（unsigned 语义）后取模。
 * 因此 key→partition 映射是稳定的：只要 partitionCount 不变，同一 key
 * 在任意节点、任意时刻、跨重启必然得到同一分区（无状态纯函数）。</p>
 */
public final class Partitioner {

    private static final ThreadLocal<MessageDigest> MD5_LOCAL = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available", e);
        }
    });

    private Partitioner() {
    }

    /**
     * 计算 key 所属分区，结果 ∈ [0, partitionCount)。
     *
     * @throws IllegalArgumentException partitionCount &lt;= 0
     */
    public static int partition(String key, int partitionCount) {
        Objects.requireNonNull(key, "key");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0, got " + partitionCount);
        }
        long unsigned = hash(key) & 0x7FFFFFFFFFFFFFFFL;
        return (int) (unsigned % partitionCount);
    }

    private static long hash(String input) {
        MessageDigest md = MD5_LOCAL.get();
        md.reset();
        byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
        long h = 0;
        for (int i = 0; i < 8; i++) {
            h = (h << 8) | (digest[i] & 0xFF);
        }
        return h;
    }
}
