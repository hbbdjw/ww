package com.ww.cache;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stream 中的单条消息：ID + 字段对（KV，类似 Redis Hash）。
 *
 * <p>不可变、线程安全；字段顺序按插入顺序保留（LinkedHashMap），与 Redis Stream 行为一致。
 * 实现 {@link Serializable} 以便通过 {@link Codec} 的 TYPE_OBJECT 路径序列化。
 */
public final class StreamEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private final StreamId id;
    private final Map<String, String> fields;

    public StreamEntry(StreamId id, Map<String, String> fields) {
        if (id == null) {
            throw new IllegalArgumentException("StreamEntry id 不能为空");
        }
        this.id = id;
        this.fields = fields == null || fields.isEmpty()
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    public StreamId getId() {
        return id;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return "StreamEntry{id=" + id + ", fields=" + fields + '}';
    }
}
