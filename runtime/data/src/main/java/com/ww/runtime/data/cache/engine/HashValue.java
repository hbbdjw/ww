package com.ww.runtime.data.cache.engine;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Hash 容器的类型标记：与 HTTP {@code type=json} 落库的普通 {@link java.util.Map}
 * 明确区分。迁移/AOF 重写/命令层只把 {@code HashValue} 当 HASH，普通 Map 仍是 KV 值。
 */
public final class HashValue<V> extends ConcurrentHashMap<String, V> {

    private static final long serialVersionUID = 1L;
}
