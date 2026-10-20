package com.ww.runtime.data.cache;

import com.ww.runtime.data.cache.engine.CacheException;
import com.ww.runtime.data.cache.engine.PelEntry;
import com.ww.runtime.data.cache.engine.PendingSummary;
import com.ww.runtime.data.cache.engine.SimpleCache;
import com.ww.runtime.data.cache.engine.StreamConsumersInfo;
import com.ww.runtime.data.cache.engine.StreamEntry;
import com.ww.runtime.data.cache.engine.StreamGroupsInfo;
import com.ww.runtime.data.cache.engine.StreamId;
import com.ww.runtime.data.cache.engine.StreamInfo;
import com.ww.runtime.data.cache.engine.StreamStore;
import com.ww.runtime.data.cache.engine.XAddOptions;
import com.ww.runtime.data.cache.engine.XAutoClaimResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 统一命令执行器（owner 本地执行）：负责参数解析、fencing、调用引擎与结果平坦化。
 *
 * <p>集群路由由 {@link CacheGateway} 负责；本类只处理已经落到本节点的命令。
 * 所有返回值转换为 Map/List/基础类型，保证 HTTP JSON 与 gRPC Codec 两条通道
 * 语义一致。</p>
 */
public final class CacheCommandExecutor {

    private final CacheCapability capability;

    public CacheCommandExecutor(CacheCapability capability) {
        this.capability = capability;
    }

    public CacheCommandResponse execute(CacheCommandRequest req) {
        long startNanos = System.nanoTime();
        SimpleCache<String, Object> metricsEngine = capability.isStarted() ? capability.commandEngine() : null;
        try {
            Object result = dispatch(req);
            record(metricsEngine, req.command(), startNanos, false);
            return CacheCommandResponse.ok(plain(result));
        } catch (CacheCommandException e) {
            record(metricsEngine, req.command(), startNanos, true);
            return CacheCommandResponse.error(e.code(), e.getMessage());
        } catch (CacheException e) {
            record(metricsEngine, req.command(), startNanos, true);
            return CacheCommandResponse.error(CacheCommandException.COMMAND_FAILED, e.getMessage());
        } catch (IllegalArgumentException | IllegalStateException e) {
            record(metricsEngine, req.command(), startNanos, true);
            return CacheCommandResponse.error(CacheCommandException.INVALID_ARGUMENT, e.getMessage());
        } catch (RuntimeException e) {
            record(metricsEngine, req.command(), startNanos, true);
            throw e;
        }
    }

    private void record(SimpleCache<String, Object> engine, String command, long startNanos, boolean error) {
        if (engine == null) {
            return;
        }
        engine.metrics().recordCommand(command, System.nanoTime() - startNanos);
        if (error) {
            engine.metrics().error();
        }
    }

    private Object dispatch(CacheCommandRequest req) {
        String command = req.command();
        if (command.isEmpty()) {
            throw invalid("command must not be empty");
        }
        switch (command) {
            case "PING":
                return "PONG";
            case "STATS":
                return capability.stats();
            case "SCAN":
                return scan(req);
            case "MGET":
                return mget(req);
            case "MSET":
                return mset(req);
            default:
                break;
        }
        List<String> keys = CacheCommandSpec.keys(req);
        if (keys.isEmpty()) {
            throw invalid(command + " requires a key");
        }
        boolean write = CacheCommandSpec.isWrite(command);
        for (String key : keys) {
            if (write) {
                capability.checkCommandWritable(key);
            } else {
                capability.checkCommandReadable(key);
            }
        }
        return executeKeyed(command, req, keys.get(0));
    }

    private Object executeKeyed(String command, CacheCommandRequest req, String key) {
        SimpleCache<String, Object> engine = capability.commandEngine();
        List<Object> args = req.args();
        switch (command) {
            // ---------------- KV / keyspace ----------------
            case "GET":
                return engine.get(key);
            case "SET": {
                requireArgs(req, 2);
                Object value = requireValue(req, 1);
                long ttl = args.size() > 2 ? lng(req, 2) : 0L;
                engine.put(key, value, ttl);
                return "OK";
            }
            case "SETNX": {
                requireArgs(req, 2);
                Object value = requireValue(req, 1);
                long ttl = args.size() > 2 ? lng(req, 2) : 0L;
                return engine.putIfAbsent(key, value, ttl);
            }
            case "DEL":
                return capability.remove(key);
            case "EXISTS":
                return engine.exists(key);
            case "TYPE":
                return engine.type(key);
            case "TTL": {
                long ttl = engine.ttlMillis(key);
                return ttl < 0 ? ttl : ttl / 1000L;
            }
            case "PTTL":
                return engine.ttlMillis(key);
            case "PERSIST":
                return engine.persist(key);
            case "TOUCH":
                return engine.touch(key);
            case "INCR":
                return engine.incrBy(key, 1L);
            case "DECR":
                return engine.incrBy(key, -1L);
            case "INCRBY":
                requireArgs(req, 2);
                return engine.incrBy(key, lng(req, 1));
            case "DECRBY":
                requireArgs(req, 2);
                return engine.decrBy(key, lng(req, 1));
            case "CAS":
                requireArgs(req, 3);
                return engine.cas(key, args.get(1), requireValue(req, 2));

            // ---------------- Hash ----------------
            case "HSET":
            case "HMSET": {
                requireArgs(req, 3);
                if ((args.size() - 1) % 2 != 0) {
                    throw invalid("HSET requires field/value pairs");
                }
                int count = 0;
                for (int i = 1; i + 1 < args.size(); i += 2) {
                    engine.hset(key, str(req, i), requireValue(req, i + 1));
                    count++;
                }
                return (long) count;
            }
            case "HGET":
                requireArgs(req, 2);
                return engine.hget(key, str(req, 1));
            case "HMGET": {
                requireArgs(req, 2);
                String[] fields = fields(req, 1);
                return engine.hmget(key, fields);
            }
            case "HGETALL":
                return engine.hgetAll(key);
            case "HDEL": {
                requireArgs(req, 2);
                return engine.hdel(key, fields(req, 1));
            }
            case "HLEN":
                return engine.hlen(key);
            case "HEXISTS":
                requireArgs(req, 2);
                return engine.hexists(key, str(req, 1));
            case "HKEYS":
                return new ArrayList<>(engine.hgetAll(key).keySet());
            case "HVALS":
                return new ArrayList<>(engine.hgetAll(key).values());

            // ---------------- List ----------------
            case "LPUSH": {
                requireArgs(req, 2);
                engine.lpush(key, requireValues(req, 1));
                return engine.llen(key);
            }
            case "RPUSH": {
                requireArgs(req, 2);
                engine.rpush(key, requireValues(req, 1));
                return engine.llen(key);
            }
            case "LPOP":
                return engine.lpop(key);
            case "RPOP":
                return engine.rpop(key);
            case "LLEN":
                return engine.llen(key);
            case "LRANGE":
                requireArgs(req, 3);
                return engine.lrange(key, lng(req, 1), lng(req, 2));
            case "LINDEX":
                requireArgs(req, 2);
                return engine.lindex(key, lng(req, 1));

            // ---------------- Set ----------------
            case "SADD":
                requireArgs(req, 2);
                return engine.saddCount(key, requireValues(req, 1));
            case "SREM":
                requireArgs(req, 2);
                return engine.sremCount(key, values(req, 1));
            case "SMEMBERS":
                return engine.smembers(key);
            case "SCARD":
                return engine.scard(key);
            case "SISMEMBER": {
                requireArgs(req, 2);
                return engine.sismember(key, args.get(1));
            }

            // ---------------- Sorted Set ----------------
            case "ZADD": {
                requireArgs(req, 3);
                if ((args.size() - 1) % 2 != 0) {
                    throw invalid("ZADD requires score/member pairs");
                }
                int count = 0;
                for (int i = 1; i + 1 < args.size(); i += 2) {
                    engine.zadd(key, dbl(req, i), requireValue(req, i + 1));
                    count++;
                }
                return (long) count;
            }
            case "ZRANGE":
                requireArgs(req, 3);
                return engine.zrange(key, lng(req, 1), lng(req, 2));
            case "ZRANGEBYSCORE":
                requireArgs(req, 3);
                return engine.zrangeByScore(key, dbl(req, 1), dbl(req, 2));
            case "ZCARD":
                return engine.zcard(key);
            case "ZSCORE":
                requireArgs(req, 2);
                return engine.zscore(key, args.get(1));
            case "ZRANK":
                requireArgs(req, 2);
                return engine.zrank(key, args.get(1));
            case "ZREM":
                requireArgs(req, 2);
                return engine.zrem(key, values(req, 1));

            // ---------------- Stream ----------------
            case "XADD":
                return xadd(req, key);
            case "XLEN":
                return engine.getStreamStore().xlen(key);
            case "XRANGE":
                return xrange(req, key, false);
            case "XREVRANGE":
                return xrange(req, key, true);
            case "XDEL": {
                requireArgs(req, 2);
                List<StreamId> ids = new ArrayList<>();
                for (int i = 1; i < args.size(); i++) {
                    ids.add(StreamId.parse(str(req, i)));
                }
                return engine.xdel(key, ids.toArray(new StreamId[0]));
            }
            case "XTRIM":
                return xtrim(req, key);
            case "XINFO":
                return xinfo(req, key);
            case "XGROUP":
                return xgroup(req, key);
            case "XACK": {
                requireArgs(req, 3);
                String group = str(req, 1);
                List<StreamId> ids = new ArrayList<>();
                for (int i = 2; i < args.size(); i++) {
                    ids.add(StreamId.parse(str(req, i)));
                }
                return engine.xack(key, group, ids.toArray(new StreamId[0]));
            }
            case "XPENDING":
                return xpending(req, key, engine);
            case "XCLAIM":
                return xclaim(req, key, engine);
            case "XAUTOCLAIM":
                return xautoclaim(req, key, engine);
            case "XREAD":
                return xread(req, key, engine);
            case "XREADGROUP":
                return xreadgroup(req, key, engine);

            default:
                throw new CacheCommandException(CacheCommandException.UNKNOWN_COMMAND,
                        "unknown command: " + command);
        }
    }

    private Object scan(CacheCommandRequest req) {
        String cursor = req.args().isEmpty() ? "" : str(req, 0);
        String prefix = null;
        int count = 100;
        List<Object> args = req.args();
        for (int i = 1; i < args.size(); i++) {
            String token = str(req, i).toUpperCase(Locale.ROOT);
            if ("MATCH".equals(token) && i + 1 < args.size()) {
                prefix = str(req, ++i);
            } else if ("COUNT".equals(token) && i + 1 < args.size()) {
                count = (int) lng(req, ++i);
            } else {
                throw invalid("SCAN supports MATCH <prefix> and COUNT <n>");
            }
        }
        return capability.commandEngine().scan(cursor, prefix, count);
    }

    private Object mget(CacheCommandRequest req) {
        requireArgs(req, 1);
        List<String> keys = new ArrayList<>();
        for (Object arg : req.args()) {
            keys.add(String.valueOf(arg));
        }
        for (String key : keys) {
            capability.checkCommandReadable(key);
        }
        return capability.commandEngine().mget(keys);
    }

    private Object mset(CacheCommandRequest req) {
        if (req.args().size() < 2 || req.args().size() % 2 != 0) {
            throw invalid("MSET requires key/value pairs");
        }
        Map<String, Object> entries = new LinkedHashMap<>();
        int firstPartition = -1;
        for (int i = 0; i < req.args().size(); i += 2) {
            String key = str(req, i);
            capability.checkCommandWritable(key);
            int partition = capability.commandPartitionOf(key);
            if (firstPartition < 0) {
                firstPartition = partition;
            } else if (firstPartition != partition) {
                throw new CacheCommandException(CacheCommandException.CROSSSLOT,
                        "MSET keys span multiple partitions");
            }
            entries.put(key, requireValue(req, i + 1));
        }
        return capability.commandEngine().mset(entries);
    }

    // ==================== Stream 命令 ====================

    private Object xadd(CacheCommandRequest req, String key) {
        List<Object> args = req.args();
        int i = 1;
        XAddOptions.Builder options = XAddOptions.builder();
        while (i < args.size()) {
            String token = str(req, i).toUpperCase(Locale.ROOT);
            if ("MAXLEN".equals(token)) {
                i++;
                if (i < args.size() && ("~".equals(str(req, i)) || "=".equals(str(req, i)))) {
                    options.approximate("~".equals(str(req, i)));
                    i++;
                }
                if (i >= args.size()) {
                    throw invalid("XADD MAXLEN requires a count");
                }
                options.maxLen(lng(req, i++));
            } else if ("NOMKSTREAM".equals(token)) {
                options.nomkstream(true);
                i++;
            } else {
                break;
            }
        }
        if (i >= args.size()) {
            throw invalid("XADD requires an id");
        }
        StreamId id = StreamId.parse(str(req, i++));
        if ((args.size() - i) % 2 != 0) {
            throw invalid("XADD requires field/value pairs");
        }
        Map<String, String> fields = new LinkedHashMap<>();
        while (i + 1 < args.size()) {
            fields.put(str(req, i), str(req, i + 1));
            i += 2;
        }
        if (fields.isEmpty()) {
            throw invalid("XADD requires at least one field/value pair");
        }
        StreamId finalId = capability.commandEngine().xadd(key, id, fields, options.build());
        return finalId == null ? null : finalId.toString();
    }

    private Object xrange(CacheCommandRequest req, String key, boolean reverse) {
        requireArgs(req, 3);
        StreamId first = StreamId.parse(str(req, 1));
        StreamId second = StreamId.parse(str(req, 2));
        long count = 0L;
        if (req.args().size() >= 5 && "COUNT".equalsIgnoreCase(str(req, 3))) {
            count = lng(req, 4);
        }
        StreamStore store = capability.commandEngine().getStreamStore();
        return reverse
                ? store.xrevrange(key, first, second, count)
                : store.xrange(key, first, second, count);
    }

    private Object xtrim(CacheCommandRequest req, String key) {
        List<Object> args = req.args();
        if (args.size() < 3 || !"MAXLEN".equalsIgnoreCase(str(req, 1))) {
            throw invalid("XTRIM supports MAXLEN <n>");
        }
        int i = 2;
        boolean approximate = false;
        if ("~".equals(str(req, i)) || "=".equals(str(req, i))) {
            approximate = "~".equals(str(req, i));
            i++;
        }
        if (i >= args.size()) {
            throw invalid("XTRIM MAXLEN requires a count");
        }
        return capability.commandEngine().xtrim(key, lng(req, i), approximate);
    }

    private Object xinfo(CacheCommandRequest req, String key) {
        requireArgs(req, 2);
        String sub = str(req, 0).toUpperCase(Locale.ROOT);
        StreamStore store = capability.commandEngine().getStreamStore();
        switch (sub) {
            case "STREAM":
                return store.xinfo(key);
            case "GROUPS":
                return store.xinfoGroups(key);
            case "CONSUMERS":
                requireArgs(req, 3);
                return store.xinfoConsumers(key, str(req, 2));
            default:
                throw invalid("XINFO supports STREAM/GROUPS/CONSUMERS");
        }
    }

    private Object xgroup(CacheCommandRequest req, String key) {
        requireArgs(req, 3);
        String sub = str(req, 0).toUpperCase(Locale.ROOT);
        SimpleCache<String, Object> engine = capability.commandEngine();
        String group = str(req, 2);
        switch (sub) {
            case "CREATE": {
                requireArgs(req, 4);
                StreamId start = StreamId.parse(str(req, 3));
                boolean mkstream = false;
                for (int i = 4; i < req.args().size(); i++) {
                    if ("MKSTREAM".equalsIgnoreCase(str(req, i))) {
                        mkstream = true;
                    }
                }
                return engine.xgroupCreate(key, group, start, mkstream);
            }
            case "DESTROY":
                return engine.xgroupDestroy(key, group);
            case "CREATECONSUMER":
                requireArgs(req, 4);
                return engine.xgroupCreateConsumer(key, group, str(req, 3));
            case "DELCONSUMER":
                requireArgs(req, 4);
                return engine.xgroupDelConsumer(key, group, str(req, 3));
            default:
                throw invalid("XGROUP supports CREATE/DESTROY/CREATECONSUMER/DELCONSUMER");
        }
    }

    private Object xpending(CacheCommandRequest req, String key, SimpleCache<String, Object> engine) {
        requireArgs(req, 2);
        String group = str(req, 1);
        int i = 2;
        long minIdle = 0L;
        if (i + 1 < req.args().size() && "IDLE".equalsIgnoreCase(str(req, i))) {
            minIdle = lng(req, i + 1);
            i += 2;
        }
        if (i >= req.args().size()) {
            return engine.getStreamStore().xpending(key, group, null, null, 0L, null, minIdle);
        }
        if (i + 2 >= req.args().size()) {
            throw invalid("XPENDING requires start/end/count");
        }
        StreamId start = StreamId.parse(str(req, i));
        StreamId end = StreamId.parse(str(req, i + 1));
        long count = lng(req, i + 2);
        String consumer = i + 3 < req.args().size() ? str(req, i + 3) : null;
        return engine.getStreamStore().xpending(key, group, start, end, count, consumer, minIdle);
    }

    private Object xclaim(CacheCommandRequest req, String key, SimpleCache<String, Object> engine) {
        requireArgs(req, 5);
        String group = str(req, 1);
        String consumer = str(req, 2);
        long minIdle = lng(req, 3);
        List<StreamId> ids = new ArrayList<>();
        for (int i = 4; i < req.args().size(); i++) {
            ids.add(StreamId.parse(str(req, i)));
        }
        return engine.xclaim(key, group, consumer, minIdle, ids.toArray(new StreamId[0]));
    }

    private Object xautoclaim(CacheCommandRequest req, String key, SimpleCache<String, Object> engine) {
        requireArgs(req, 5);
        String group = str(req, 1);
        String consumer = str(req, 2);
        long minIdle = lng(req, 3);
        StreamId start = StreamId.parse(str(req, 4));
        long count = req.args().size() > 5 ? lng(req, 5) : 100L;
        return engine.xautoclaim(key, group, consumer, minIdle, start, count);
    }

    private Object xread(CacheCommandRequest req, String key, SimpleCache<String, Object> engine) {
        long count = 0L;
        long blockMs = 0L;
        int streams = -1;
        List<Object> args = req.args();
        for (int i = 1; i < args.size(); i++) {
            String token = str(req, i).toUpperCase(Locale.ROOT);
            if ("COUNT".equals(token) && i + 1 < args.size()) {
                count = lng(req, ++i);
            } else if ("BLOCK".equals(token) && i + 1 < args.size()) {
                blockMs = Math.min(30_000L, Math.max(0L, lng(req, ++i)));
            } else if ("STREAMS".equals(token)) {
                streams = i;
                break;
            } else {
                throw invalid("XREAD supports COUNT/BLOCK/STREAMS");
            }
        }
        if (streams < 0 || streams + 2 >= args.size()) {
            throw invalid("XREAD requires STREAMS <key> <id>");
        }
        String idStr = str(req, streams + 2);
        StreamId from;
        if ("$".equals(idStr)) {
            StreamInfo info = engine.getStreamStore().xinfo(key);
            from = info == null || info.getLastId() == null ? StreamId.MIN : info.getLastId();
        } else {
            from = StreamId.parse(idStr);
        }
        try {
            return engine.getStreamStore().xread(key, from, count, blockMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheCommandException(CacheCommandException.COMMAND_FAILED, "XREAD interrupted");
        }
    }

    private Object xreadgroup(CacheCommandRequest req, String key, SimpleCache<String, Object> engine) {
        requireArgs(req, 3);
        if (!"GROUP".equalsIgnoreCase(str(req, 0))) {
            throw invalid("XREADGROUP requires GROUP <group> <consumer>");
        }
        String group = str(req, 1);
        String consumer = str(req, 2);
        long count = 0L;
        long blockMs = 0L;
        int streams = -1;
        List<Object> args = req.args();
        for (int i = 3; i < args.size(); i++) {
            String token = str(req, i).toUpperCase(Locale.ROOT);
            if ("COUNT".equals(token) && i + 1 < args.size()) {
                count = lng(req, ++i);
            } else if ("BLOCK".equals(token) && i + 1 < args.size()) {
                blockMs = Math.min(30_000L, Math.max(0L, lng(req, ++i)));
            } else if ("STREAMS".equals(token)) {
                streams = i;
                break;
            } else {
                throw invalid("XREADGROUP supports GROUP/COUNT/BLOCK/STREAMS");
            }
        }
        if (streams < 0 || streams + 2 >= args.size()) {
            throw invalid("XREADGROUP requires STREAMS <key> <id>");
        }
        String idStr = str(req, streams + 2);
        boolean readNew = ">".equals(idStr);
        StreamId from = readNew ? StreamId.MIN : StreamId.parse(idStr);
        try {
            return engine.xreadGroup(key, group, consumer, readNew, from, count, blockMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheCommandException(CacheCommandException.COMMAND_FAILED, "XREADGROUP interrupted");
        }
    }

    // ==================== 参数与结果工具 ====================

    private static void requireArgs(CacheCommandRequest req, int min) {
        if (req.args().size() < min) {
            throw invalid(req.command() + " requires at least " + min + " argument(s)");
        }
    }

    private static CacheCommandException invalid(String message) {
        return new CacheCommandException(CacheCommandException.INVALID_ARGUMENT, message);
    }

    private static Object requireValue(CacheCommandRequest req, int index) {
        Object value = req.args().get(index);
        if (value == null) {
            throw invalid("argument " + index + " must not be null");
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <V> V[] requireValues(CacheCommandRequest req, int from) {
        for (int i = from; i < req.args().size(); i++) {
            requireValue(req, i);
        }
        return values(req, from);
    }

    private static String str(CacheCommandRequest req, int index) {
        if (index < 0 || index >= req.args().size()) {
            throw invalid("missing argument " + index);
        }
        Object value = req.args().get(index);
        if (value == null) {
            throw invalid("argument " + index + " must not be null");
        }
        return String.valueOf(value);
    }

    private static long lng(CacheCommandRequest req, int index) {
        Object value = req.args().get(index);
        if (value instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw invalid("argument " + index + " is not an integer");
        }
    }

    private static double dbl(CacheCommandRequest req, int index) {
        Object value = req.args().get(index);
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw invalid("argument " + index + " is not a number");
        }
    }

    private static String[] fields(CacheCommandRequest req, int from) {
        List<String> fields = new ArrayList<>();
        for (int i = from; i < req.args().size(); i++) {
            fields.add(str(req, i));
        }
        return fields.toArray(new String[0]);
    }

    @SuppressWarnings("unchecked")
    private static <V> V[] values(CacheCommandRequest req, int from) {
        List<Object> values = new ArrayList<>();
        for (int i = from; i < req.args().size(); i++) {
            values.add(req.args().get(i));
        }
        return (V[]) values.toArray();
    }

    private static Object plain(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof StreamEntry entry) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", entry.getId().toString());
            out.put("fields", entry.getFields());
            return out;
        }
        if (value instanceof PelEntry pel) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", pel.getId().toString());
            out.put("consumer", pel.getConsumer());
            out.put("deliveredTimeMs", pel.getDeliveredTimeMs());
            out.put("deliveryCount", pel.getDeliveryCount());
            out.put("idleMs", pel.idleTimeMs(System.currentTimeMillis()));
            return out;
        }
        if (value instanceof PendingSummary pending) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("total", pending.getTotal());
            out.put("firstId", pending.getFirstId() == null ? null : pending.getFirstId().toString());
            out.put("lastId", pending.getLastId() == null ? null : pending.getLastId().toString());
            out.put("consumers", pending.getConsumerCounts());
            out.put("entries", pending.getEntries() == null ? null : plain(pending.getEntries()));
            out.put("detailed", pending.isDetailed());
            return out;
        }
        if (value instanceof StreamInfo info) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("length", info.getLength());
            out.put("firstId", info.getFirstId() == null ? null : info.getFirstId().toString());
            out.put("lastId", info.getLastId() == null ? null : info.getLastId().toString());
            out.put("firstEntry", plain(info.getFirstEntry()));
            out.put("lastEntry", plain(info.getLastEntry()));
            out.put("groups", info.getGroupsCount());
            return out;
        }
        if (value instanceof StreamGroupsInfo groups) {
            List<Object> list = new ArrayList<>();
            for (StreamGroupsInfo.GroupSummary g : groups.getGroups()) {
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("name", g.getName());
                out.put("consumers", g.getConsumers());
                out.put("pending", g.getPending());
                out.put("lastDeliveredId", g.getLastDeliveredId() == null
                        ? null : g.getLastDeliveredId().toString());
                list.add(out);
            }
            return list;
        }
        if (value instanceof StreamConsumersInfo consumers) {
            List<Object> list = new ArrayList<>();
            for (StreamConsumersInfo.ConsumerSummary c : consumers.getConsumers()) {
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("name", c.getName());
                out.put("pending", c.getPending());
                out.put("idleMs", c.getIdleMs());
                list.add(out);
            }
            return list;
        }
        if (value instanceof XAutoClaimResult auto) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("nextCursor", auto.getNextCursor() == null ? null : auto.getNextCursor().toString());
            out.put("claimed", plain(auto.getClaimed()));
            return out;
        }
        if (value instanceof StreamId id) {
            return id.toString();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.put(String.valueOf(e.getKey()), plain(e.getValue()));
            }
            return out;
        }
        if (value instanceof Set<?> set) {
            List<Object> out = new ArrayList<>(set.size());
            for (Object item : set) {
                out.add(plain(item));
            }
            return out;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> out = new ArrayList<>();
            for (Object item : iterable) {
                out.add(plain(item));
            }
            return out;
        }
        return value;
    }
}
