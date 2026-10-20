package com.ww.runtime.data.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * CACHE 统一命令请求：Redis 风格命令名 + 位置参数。
 *
 * <p>参数值支持 JSON 基础类型、Map/List/Set 与可序列化对象；gRPC 通道用
 * {@code Codec} 编码整个参数列表，HTTP 通道直接使用 JSON 数组。</p>
 */
public record CacheCommandRequest(String command, List<Object> args) {

    public CacheCommandRequest {
        command = command == null ? "" : command.trim().toUpperCase(Locale.ROOT);
        args = args == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(args));
    }

    public static CacheCommandRequest of(String command, Object... args) {
        List<Object> list = new ArrayList<>(args.length);
        Collections.addAll(list, args);
        return new CacheCommandRequest(command, list);
    }

    public String arg(int index) {
        Object value = args.get(index);
        return value == null ? null : String.valueOf(value);
    }
}
