package com.ww.cache;

import java.io.Serializable;

/**
 * Stream 消息 ID，由两部分组成：毫秒时间戳 + 序列号，格式与 Redis Stream 兼容（如 "1700000000000-0"）。
 *
 * <p>比较顺序：先比较 msTime，再比较 seq。{@link #MIN} 与 {@link #MAX} 提供范围查询通配边界，
 * 对应 Redis XRANGE 中的 {@code -} 与 {@code +}。
 *
 * <p>不可变、线程安全；实现 {@link Serializable} 以便通过 {@link Codec} 的 TYPE_OBJECT 路径序列化。
 */
public final class StreamId implements Comparable<StreamId>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 范围查询下界，等价 Redis 的 {@code -}。 */
    public static final StreamId MIN = new StreamId(0L, 0L);
    /** 范围查询上界，等价 Redis 的 {@code +}。 */
    public static final StreamId MAX = new StreamId(Long.MAX_VALUE, Long.MAX_VALUE);
    /** 自动生成 ID 标记：客户端发送此 ID 表示请求服务端生成。 */
    public static final StreamId AUTO = new StreamId(-1L, -1L);

    private final long msTime;
    private final long seq;

    public StreamId(long msTime, long seq) {
        this.msTime = msTime;
        this.seq = seq;
    }

    public long getMsTime() {
        return msTime;
    }

    public long getSeq() {
        return seq;
    }

    /**
     * 解析字符串形式的 Stream ID，支持：
     * <ul>
     *   <li>{@code "-"} → {@link #MIN}</li>
     *   <li>{@code "+"} → {@link #MAX}</li>
     *   <li>{@code "*"} → {@link #AUTO}</li>
     *   <li>{@code "1700000000000-0"} → (1700000000000, 0)</li>
     *   <li>{@code "1700000000000"} → (1700000000000, 0)</li>
     * </ul>
     */
    public static StreamId parse(String s) {
        if (s == null || s.isEmpty() || "-".equals(s)) return MIN;
        if ("+".equals(s)) return MAX;
        if ("*".equals(s)) return AUTO;
        int dash = s.indexOf('-');
        if (dash < 0) {
            return new StreamId(Long.parseLong(s.trim()), 0L);
        }
        long ms = Long.parseLong(s.substring(0, dash).trim());
        long sq = Long.parseLong(s.substring(dash + 1).trim());
        return new StreamId(ms, sq);
    }

    /**
     * 基于 lastId 自动生成下一个 ID：
     * <ul>
     *   <li>当前毫秒大于 lastId.msTime → (now, 0)</li>
     *   <li>当前毫秒等于 lastId.msTime → (now, lastId.seq + 1)</li>
     *   <li>时钟回拨（当前小于 lastId） → (lastId.msTime, lastId.seq + 1)，保证单调递增</li>
     * </ul>
     */
    public static StreamId nextAfter(StreamId lastId) {
        long now = System.currentTimeMillis();
        if (lastId == null) {
            return new StreamId(now, 0L);
        }
        if (now > lastId.msTime) {
            return new StreamId(now, 0L);
        }
        // 相同毫秒或时钟回拨：基于 lastId 自增 seq 保证单调
        return new StreamId(lastId.msTime, lastId.seq + 1);
    }

    @Override
    public int compareTo(StreamId o) {
        int c = Long.compare(msTime, o.msTime);
        return c != 0 ? c : Long.compare(seq, o.seq);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamId other)) return false;
        return msTime == other.msTime && seq == other.seq;
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(msTime) + Long.hashCode(seq);
    }

    @Override
    public String toString() {
        return msTime + "-" + seq;
    }
}
