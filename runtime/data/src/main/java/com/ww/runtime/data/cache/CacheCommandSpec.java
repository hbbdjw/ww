package com.ww.runtime.data.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 命令路由元数据：key 位置、读写属性、是否无 key。
 *
 * <p>与执行器解耦，网关在转发前用它判断属主；新增命令必须同时在此登记，
 * 避免“本地能执行、集群不路由”的隐性分裂。</p>
 */
public final class CacheCommandSpec {

    private static final Set<String> KEYLESS = Set.of("PING", "STATS", "SCAN");
    private static final Set<String> WRITE = Set.of(
            "SET", "SETNX", "DEL", "PERSIST", "INCR", "DECR", "INCRBY", "DECRBY", "CAS",
            "MSET", "HSET", "HMSET", "HDEL", "LPUSH", "RPUSH", "LPOP", "RPOP",
            "SADD", "SREM", "ZADD", "ZREM",
            "XADD", "XDEL", "XTRIM", "XGROUP", "XACK", "XCLAIM", "XAUTOCLAIM", "XREADGROUP"
    );

    private CacheCommandSpec() {
    }

    public static boolean isKeyless(String command) {
        return KEYLESS.contains(command);
    }

    public static boolean isWrite(String command) {
        return WRITE.contains(command);
    }

    /** 命令涉及的 key；MGET 全部参数、MSET 偶数位、XREAD/XREADGROUP 取 STREAMS 后第一个。 */
    public static List<String> keys(CacheCommandRequest req) {
        String command = req.command();
        List<Object> args = req.args();
        if (isKeyless(command) || args.isEmpty()) {
            return List.of();
        }
        if ("MGET".equals(command)) {
            return stringArgs(args, 0, args.size());
        }
        if ("MSET".equals(command)) {
            List<String> keys = new ArrayList<>();
            for (int i = 0; i + 1 < args.size(); i += 2) {
                keys.add(String.valueOf(args.get(i)));
            }
            return keys;
        }
        if ("XGROUP".equals(command) || "XINFO".equals(command)) {
            return args.size() > 1 ? List.of(String.valueOf(args.get(1))) : List.of();
        }
        if ("XREAD".equals(command) || "XREADGROUP".equals(command)) {
            int streams = indexOf(args, "STREAMS");
            return streams >= 0 && streams + 1 < args.size()
                    ? List.of(String.valueOf(args.get(streams + 1)))
                    : List.of();
        }
        return List.of(String.valueOf(args.get(0)));
    }

    private static List<String> stringArgs(List<Object> args, int from, int to) {
        List<String> out = new ArrayList<>();
        for (int i = from; i < to; i++) {
            out.add(String.valueOf(args.get(i)));
        }
        return out;
    }

    private static int indexOf(List<Object> args, String token) {
        for (int i = 0; i < args.size(); i++) {
            Object value = args.get(i);
            if (value != null && token.equals(String.valueOf(value).toUpperCase(Locale.ROOT))) {
                return i;
            }
        }
        return -1;
    }
}
