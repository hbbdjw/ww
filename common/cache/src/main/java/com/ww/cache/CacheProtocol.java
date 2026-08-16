package com.ww.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TCP 自定义二进制帧协议
 *
 * <pre>
 * 请求帧:  [MAGIC 4B] [VERSION 1B] [FLAGS 1B] [OPCODE 1B] [REQ_ID 4B] [PAYLOAD_LEN 4B] [PAYLOAD]
 * 响应帧:  [MAGIC 4B] [VERSION 1B] [FLAGS 1B] [STATUS 1B] [REQ_ID 4B] [PAYLOAD_LEN 4B] [PAYLOAD]
 * </pre>
 */
final class CacheProtocol {

    static final int MAGIC = 0x57434143; // "WCAC"
    static final byte VERSION = 0x01;

    // 帧标志
    static final byte FLAG_NORMAL  = 0x00;
    static final byte FLAG_REPLICA = 0x01; // 副本写入，不触发 HRW 转发和复制

    // 状态码
    static final byte STATUS_OK    = 0x00;
    static final byte STATUS_ERROR = 0x01;
    static final byte STATUS_NULL  = 0x02;

    // 操作码
    static final byte OP_PUT           = 0x01;
    static final byte OP_PUT_EXPIRED   = 0x02;
    static final byte OP_GET           = 0x03;
    static final byte OP_REMOVE        = 0x04;
    static final byte OP_CLEAR         = 0x05;
    static final byte OP_SIZE          = 0x06;
    static final byte OP_KEYS          = 0x07;
    static final byte OP_HSET          = 0x10;
    static final byte OP_HGET          = 0x11;
    static final byte OP_HMGET         = 0x12;
    static final byte OP_HGETALL       = 0x13;
    static final byte OP_LPUSH         = 0x20;
    static final byte OP_RPUSH         = 0x21;
    static final byte OP_LPOP          = 0x22;
    static final byte OP_RPOP          = 0x23;
    static final byte OP_SADD          = 0x30;
    static final byte OP_SMEMBERS      = 0x31;
    static final byte OP_SREM          = 0x32;
    static final byte OP_ZADD          = 0x40;
    static final byte OP_ZRANGE        = 0x41;
    static final byte OP_ZRANGEBYSCORE = 0x42;
    static final byte OP_LIST_REPLACE  = 0x50; // 列表状态同步（lpop/rpop 复制用）
    static final byte OP_CLEAR_OWNED_ONLY = 0x51; // 仅清除本节点 owner 集合 {@code 缺陷8修复}
    // ==================== Stream 操作 ====================
    static final byte OP_XADD          = 0x60;
    static final byte OP_XLEN          = 0x61;
    static final byte OP_XRANGE        = 0x62;
    static final byte OP_XREVRANGE     = 0x63;
    static final byte OP_XDEL          = 0x64;
    static final byte OP_XTRIM         = 0x65;
    static final byte OP_XINFO         = 0x66;
    static final byte OP_XREAD         = 0x67; // 阻塞 XREAD
    static final byte OP_XGROUP_CREATE      = 0x70;
    static final byte OP_XGROUP_DESTROY     = 0x71;
    static final byte OP_XGROUP_CREATECONSUMER = 0x72;
    static final byte OP_XGROUP_DELCONSUMER = 0x73;
    static final byte OP_XREADGROUP    = 0x74;
    static final byte OP_XACK          = 0x75;
    static final byte OP_XPENDING      = 0x76;
    static final byte OP_XCLAIM        = 0x77;
    static final byte OP_XAUTOCLAIM    = 0x78;
    static final byte OP_XINFO_GROUPS  = 0x79;
    static final byte OP_XINFO_CONSUMERS = 0x7A;
    static final byte OP_MULTI         = (byte) 0xF0;
    static final byte OP_EXEC          = (byte) 0xF1;
    static final byte OP_DISCARD       = (byte) 0xF2;
    static final byte OP_WATCH         = (byte) 0xF3;
    static final byte OP_AUTH          = (byte) 0xFE; // 认证握手

    // ==================== 集群管理操作 (0x80-0x9F) ====================
    static final byte OP_PING          = (byte) 0x80; // gossip ping：携带发送方 membership 快照
    static final byte OP_PONG          = (byte) 0x81; // gossip pong：回应 membership 快照
    static final byte OP_NODE_JOIN     = (byte) 0x82; // 新节点加入请求
    static final byte OP_NODE_LEAVE    = (byte) 0x83; // 节点主动退出
    static final byte OP_PROMOTE       = (byte) 0x84; // failover 提升：通知某节点成为新 primary
    static final byte OP_MIGRATE_PULL  = (byte) 0x85; // 迁移：拉取属于指定 owner 范围的 key 快照
    static final byte OP_MIGRATE_PUSH  = (byte) 0x86; // 迁移：推送 key 快照到目标节点
    static final byte OP_CLUSTER_STATUS= (byte) 0x87; // 查询集群状态
    static final byte OP_GOSSIP        = (byte) 0x88; // gossip 同步 membership 增量

    static final int HEADER_SIZE = 15; // 4 + 1 + 1 + 1 + 4 + 4

    private CacheProtocol() {}

    // ==================== 帧读写 ====================

    record Frame(byte version, byte flags, byte opOrStatus, int requestId, byte[] payload) {}

    static byte[] encodeFrame(byte flags, byte opOrStatus, int requestId, byte[] payload) {
        int payloadLen = (payload != null) ? payload.length : 0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream(HEADER_SIZE + payloadLen);
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(flags);
            out.writeByte(opOrStatus);
            out.writeInt(requestId);
            out.writeInt(payloadLen);
            if (payloadLen > 0) out.write(payload);
        } catch (IOException e) {
            throw new RuntimeException("encode frame error", e);
        }
        return bos.toByteArray();
    }

    static byte[] encodeRequest(byte flags, byte opCode, int requestId, byte[] payload) {
        return encodeFrame(flags, opCode, requestId, payload);
    }

    static byte[] encodeRequest(byte opCode, int requestId, byte[] payload) {
        return encodeFrame(FLAG_NORMAL, opCode, requestId, payload);
    }

    static byte[] encodeResponse(byte flags, byte status, int requestId, byte[] payload) {
        return encodeFrame(flags, status, requestId, payload);
    }

    static byte[] encodeResponse(byte status, int requestId, byte[] payload) {
        return encodeFrame(FLAG_NORMAL, status, requestId, payload);
    }

    static Frame decodeFrame(byte[] frame) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame));
        try {
            int magic = in.readInt();
            if (magic != MAGIC) throw new RuntimeException("invalid magic: " + Integer.toHexString(magic));
            byte version = in.readByte();
            byte flags = in.readByte();
            byte opOrStatus = in.readByte();
            int requestId = in.readInt();
            int payloadLen = in.readInt();
            byte[] payload = new byte[payloadLen];
            if (payloadLen > 0) in.readFully(payload);
            return new Frame(version, flags, opOrStatus, requestId, payload);
        } catch (IOException e) {
            throw new RuntimeException("decode frame error", e);
        }
    }

    // ==================== 命令参数编码 ====================

    static byte[] encodeArgs(byte opCode, Object... args) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(opCode);
            for (Object arg : args) {
                out.write(Codec.encodeValue(arg));
            }
        } catch (IOException e) {
            throw new RuntimeException("encode args error", e);
        }
        return bos.toByteArray();
    }

    // ==================== 在本地缓存上执行命令并编码结果 ====================

    @SuppressWarnings("unchecked")
    static byte[] executeAndEncode(Cache<String, Object> cache, byte opCode, byte[] payload) {
        Object result = executeCommand(cache, opCode, payload);
        if (result == null) {
            return encodeResponse(STATUS_NULL, 0, new byte[0]);
        }
        return encodeResponse(STATUS_OK, 0, Codec.encodeValue(result));
    }

    static Object executeCommand(Cache<String, Object> cache, byte opCode, byte[] payload) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        try {
            in.readByte(); // skip opCode (already known)
            return switch (opCode) {
                case OP_PUT          -> { String k = readString(in); Object v = readNextValue(in); cache.put(k, v); yield null; }
                case OP_PUT_EXPIRED  -> { String k = readString(in); Object v = readNextValue(in); long ttl = readNextLong(in); cache.put(k, v, ttl); yield null; }
                case OP_GET          -> cache.get(readString(in));
                case OP_REMOVE       -> { cache.remove(readString(in)); yield null; }
                case OP_CLEAR        -> { cache.clear(); yield null; }
                case OP_SIZE         -> cache.size();
                case OP_KEYS         -> cache.keys();
                case OP_HSET         -> { cache.hset(readString(in), readString(in), readNextValue(in)); yield null; }
                case OP_HGET         -> cache.hget(readString(in), readString(in));
                case OP_HMGET        -> cache.hmget(readString(in), readStringArray(in));
                case OP_HGETALL      -> cache.hgetAll(readString(in));
                case OP_LPUSH        -> { cache.lpush(readString(in), readValueArray(in)); yield null; }
                case OP_RPUSH        -> { cache.rpush(readString(in), readValueArray(in)); yield null; }
                case OP_LPOP         -> cache.lpop(readString(in));
                case OP_RPOP         -> cache.rpop(readString(in));
                case OP_SADD         -> { cache.sadd(readString(in), readValueArray(in)); yield null; }
                case OP_SMEMBERS     -> cache.smembers(readString(in));
                case OP_SREM         -> { cache.srem(readString(in), readValueArray(in)); yield null; }
                case OP_ZADD         -> { cache.zadd(readString(in), readNextDouble(in), readNextValue(in)); yield null; }
                case OP_ZRANGE       -> cache.zrange(readString(in), readNextLong(in), readNextLong(in));
                case OP_ZRANGEBYSCORE -> cache.zrangeByScore(readString(in), readNextDouble(in), readNextDouble(in));
                case OP_LIST_REPLACE  -> {
                    String k = readString(in);
                    Object listObj = readNextValue(in);
                    long remainingTtl;
                    Object ttlObj = Codec.decodeValue(in);
                    if (ttlObj instanceof Number n) {
                        remainingTtl = n.longValue();
                    } else {
                        remainingTtl = Long.MAX_VALUE;
                    }
                    @SuppressWarnings("unchecked")
                    List<Object> items = (listObj instanceof List) ? (List<Object>) listObj : new java.util.ArrayList<>();
                    if (cache instanceof SimpleCache<String, Object> sc) {
                        sc.listReplace(k, items, remainingTtl);
                    }
                    yield null;
                }
                case OP_MULTI        -> { cache.multi(); yield null; }
                case OP_EXEC         -> cache.exec();
                case OP_DISCARD      -> { cache.discard(); yield null; }
                case OP_WATCH        -> { cache.watch(readStringArray(in)); yield null; }
                // ==================== Stream 操作 ====================
                case OP_XADD         -> executeXAdd(cache, in);
                case OP_XLEN         -> executeXLen(cache, in);
                case OP_XRANGE       -> executeXRange(cache, in, false);
                case OP_XREVRANGE    -> executeXRange(cache, in, true);
                case OP_XDEL         -> executeXDel(cache, in);
                case OP_XTRIM        -> executeXTrim(cache, in);
                case OP_XINFO        -> executeXInfo(cache, in);
                case OP_XREAD        -> executeXRead(cache, in);
                case OP_XGROUP_CREATE      -> executeXGroupCreate(cache, in);
                case OP_XGROUP_DESTROY     -> executeXGroupDestroy(cache, in);
                case OP_XGROUP_CREATECONSUMER -> executeXGroupCreateConsumer(cache, in);
                case OP_XGROUP_DELCONSUMER -> executeXGroupDelConsumer(cache, in);
                case OP_XREADGROUP   -> executeXReadGroup(cache, in);
                case OP_XACK         -> executeXAck(cache, in);
                case OP_XPENDING     -> executeXPending(cache, in);
                case OP_XCLAIM       -> executeXClaim(cache, in);
                case OP_XAUTOCLAIM   -> executeXAutoClaim(cache, in);
                case OP_XINFO_GROUPS -> executeXInfoGroups(cache, in);
                case OP_XINFO_CONSUMERS -> executeXInfoConsumers(cache, in);
                default -> throw new RuntimeException("unknown opCode: " + Integer.toHexString(opCode & 0xFF));
            };
        } catch (IOException e) {
            throw new RuntimeException("decode command error", e);
        }
    }

    // ==================== 辅助读方法 ====================

    private static String readString(DataInputStream in) throws IOException {
        return (String) Codec.decodeValue(in);
    }

    private static String[] readStringArray(DataInputStream in) throws IOException {
        Object obj = Codec.decodeValue(in);
        if (obj instanceof List<?> list) {
            return list.stream().map(String::valueOf).toArray(String[]::new);
        }
        return new String[0];
    }

    private static Object[] readValueArray(DataInputStream in) throws IOException {
        Object obj = Codec.decodeValue(in);
        if (obj instanceof List<?> list) return list.toArray();
        return new Object[0];
    }

    private static double readNextDouble(DataInputStream in) throws IOException {
        Object obj = Codec.decodeValue(in);
        if (obj instanceof Number n) return n.doubleValue();
        return 0;
    }

    private static long readNextLong(DataInputStream in) throws IOException {
        Object obj = Codec.decodeValue(in);
        if (obj instanceof Number n) return n.longValue();
        return 0;
    }

    private static Object readNextValue(DataInputStream in) throws IOException {
        return Codec.decodeValue(in);
    }

    // ==================== Stream 命令执行 ====================

    /**
     * 从 Cache 取出 StreamStore（仅 SimpleCache 支持）；其他实现返回 null。
     */
    private static StreamStore streamStoreOf(Cache<String, Object> cache) {
        if (cache instanceof SimpleCache<?, ?> sc) {
            return sc.getStreamStore();
        }
        return null;
    }

    /**
     * XADD payload 布局：
     * <pre>[opCode][streamKey][idMs][idSeq][maxLen][approximate][nomkstream][fieldsCount][field0Key][field0Val]...</pre>
     * idMs/idSeq 均为 -1 时表示 AUTO 由服务端生成；maxLen &lt;=0 表示不裁剪。
     */
    @SuppressWarnings("unchecked")
    private static StreamId executeXAdd(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        long idMs = readNextLong(in);
        long idSeq = readNextLong(in);
        long maxLen = readNextLong(in);
        boolean approximate = readBoolean(in);
        boolean nomkstream = readBoolean(in);
        int fieldCount = in.readInt();
        Map<String, String> fields = new LinkedHashMap<>(fieldCount * 2);
        for (int i = 0; i < fieldCount; i++) {
            String fk = readString(in);
            String fv = readString(in);
            fields.put(fk, fv);
        }
        StreamId idReq = (idMs == -1L && idSeq == -1L) ? StreamId.AUTO : new StreamId(idMs, idSeq);
        XAddOptions opts = XAddOptions.builder()
                .maxLen(maxLen)
                .approximate(approximate)
                .nomkstream(nomkstream)
                .build();
        // 走 SimpleCache 包装路径以记录 AOF；非 SimpleCache 实现降级为直接调 streamStore
        if (cache instanceof SimpleCache<?, ?> sc) {
            return ((SimpleCache<String, Object>) sc).xadd(key, idReq, fields, opts);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XADD 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xadd(key, idReq, fields, opts);
    }

    private static long executeXLen(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XLEN 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xlen(key);
    }

    /**
     * XRANGE / XREVRANGE payload 布局：
     * <pre>[opCode][streamKey][minMs][minSeq][maxMs][maxSeq][count]</pre>
     * min/max 等于 (0,0)/(Long.MAX_VALUE,Long.MAX_VALUE) 表示边界通配。
     */
    private static List<StreamEntry> executeXRange(Cache<String, Object> cache, DataInputStream in, boolean reverse) throws IOException {
        String key = readString(in);
        long minMs = readNextLong(in);
        long minSeq = readNextLong(in);
        long maxMs = readNextLong(in);
        long maxSeq = readNextLong(in);
        long count = readNextLong(in);
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XRANGE 不支持当前 Cache 实现: " + cache.getClass());
        }
        StreamId min = new StreamId(minMs, minSeq);
        StreamId max = new StreamId(maxMs, maxSeq);
        return reverse
                ? store.xrevrange(key, max, min, count)
                : store.xrange(key, min, max, count);
    }

    /**
     * XDEL payload 布局：
     * <pre>[opCode][streamKey][idCount][id0Ms][id0Seq][id1Ms][id1Seq]...</pre>
     */
    private static long executeXDel(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        int idCount = in.readInt();
        StreamId[] ids = new StreamId[idCount];
        for (int i = 0; i < idCount; i++) {
            long ms = readNextLong(in);
            long sq = readNextLong(in);
            ids[i] = new StreamId(ms, sq);
        }
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xdel(key, ids);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XDEL 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xdel(key, ids);
    }

    /**
     * XTRIM payload 布局：
     * <pre>[opCode][streamKey][maxLen][approximate]</pre>
     */
    private static long executeXTrim(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        long maxLen = readNextLong(in);
        boolean approximate = readBoolean(in);
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xtrim(key, maxLen, approximate);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XTRIM 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xtrim(key, maxLen, approximate);
    }

    private static StreamInfo executeXInfo(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XINFO 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xinfo(key);
    }

    /**
     * XREAD payload 布局：[opCode][streamKey][fromMs][fromSeq][count][blockMs]
     * blockMs &lt;= 0 表示非阻塞。
     */
    private static List<StreamEntry> executeXRead(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        long fromMs = readNextLong(in);
        long fromSeq = readNextLong(in);
        long count = readNextLong(in);
        long blockMs = readNextLong(in);
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XREAD 不支持当前 Cache 实现: " + cache.getClass());
        }
        try {
            return store.xread(key, new StreamId(fromMs, fromSeq), count, blockMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ArrayList<>();
        }
    }

    /**
     * XGROUP CREATE payload：[opCode][key][groupName][startMs][startSeq][mkstream]
     */
    private static boolean executeXGroupCreate(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        long startMs = readNextLong(in);
        long startSeq = readNextLong(in);
        boolean mkstream = readBoolean(in);
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XGROUP CREATE 不支持当前 Cache 实现: " + cache.getClass());
        }
        StreamId startId = new StreamId(startMs, startSeq);
        // 走 SimpleCache 包装路径以记录 AOF
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xgroupCreate(key, group, startId, mkstream);
        }
        return store.xgroupCreate(key, group, startId, mkstream);
    }

    private static boolean executeXGroupDestroy(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        // 走 SimpleCache 包装路径以记录 AOF
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xgroupDestroy(key, group);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XGROUP DESTROY 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xgroupDestroy(key, group);
    }

    private static int executeXGroupCreateConsumer(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        String consumer = readString(in);
        // 走 SimpleCache 包装路径以记录 AOF
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xgroupCreateConsumer(key, group, consumer);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XGROUP CREATECONSUMER 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xgroupCreateConsumer(key, group, consumer);
    }

    private static long executeXGroupDelConsumer(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        String consumer = readString(in);
        // 走 SimpleCache 包装路径以记录 AOF
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xgroupDelConsumer(key, group, consumer);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XGROUP DELCONSUMER 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xgroupDelConsumer(key, group, consumer);
    }

    /**
     * XREADGROUP payload：[opCode][key][group][consumer][readNew][fromMs][fromSeq][count][blockMs]
     */
    private static List<StreamEntry> executeXReadGroup(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        String consumer = readString(in);
        boolean readNew = readBoolean(in);
        long fromMs = readNextLong(in);
        long fromSeq = readNextLong(in);
        long count = readNextLong(in);
        long blockMs = readNextLong(in);
        // 走 SimpleCache 包装路径以记录 AOF（投递事件、PEL 变更）
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            try {
                return simple.xreadGroup(key, group, consumer, readNew,
                        new StreamId(fromMs, fromSeq), count, blockMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new ArrayList<>();
            }
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XREADGROUP 不支持当前 Cache 实现: " + cache.getClass());
        }
        try {
            return store.xreadGroup(key, group, consumer, readNew,
                    new StreamId(fromMs, fromSeq), count, blockMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ArrayList<>();
        }
    }

    /**
     * XACK payload：[opCode][key][group][idCount][id0Ms][id0Seq]...
     */
    private static long executeXAck(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        int idCount = in.readInt();
        StreamId[] ids = new StreamId[idCount];
        for (int i = 0; i < idCount; i++) {
            ids[i] = new StreamId(readNextLong(in), readNextLong(in));
        }
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xack(key, group, ids);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XACK 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xack(key, group, ids);
    }

    /**
     * XPENDING payload：[opCode][key][group][startMs][startSeq][endMs][endSeq][count][consumerName][minIdleMs]
     * consumerName 为空字符串表示无 consumer 参数（概要形态）。
     */
    private static PendingSummary executeXPending(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        long startMs = readNextLong(in);
        long startSeq = readNextLong(in);
        long endMs = readNextLong(in);
        long endSeq = readNextLong(in);
        long count = readNextLong(in);
        String consumer = readString(in);
        long minIdleMs = readNextLong(in);
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XPENDING 不支持当前 Cache 实现: " + cache.getClass());
        }
        StreamId start = new StreamId(startMs, startSeq);
        StreamId end = new StreamId(endMs, endSeq);
        String consumerArg = (consumer == null || consumer.isEmpty()) ? null : consumer;
        return store.xpending(key, group, start, end, count, consumerArg, minIdleMs);
    }

    /**
     * XCLAIM payload：[opCode][key][group][consumer][minIdleMs][idCount][id0Ms][id0Seq]...
     */
    private static List<PelEntry> executeXClaim(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        String consumer = readString(in);
        long minIdleMs = readNextLong(in);
        int idCount = in.readInt();
        StreamId[] ids = new StreamId[idCount];
        for (int i = 0; i < idCount; i++) {
            ids[i] = new StreamId(readNextLong(in), readNextLong(in));
        }
        // 走 SimpleCache 包装路径以记录 AOF
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xclaim(key, group, consumer, minIdleMs, ids);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XCLAIM 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xclaim(key, group, consumer, minIdleMs, ids);
    }

    /**
     * XAUTOCLAIM payload：[opCode][key][group][consumer][minIdleMs][startMs][startSeq][count]
     */
    private static XAutoClaimResult executeXAutoClaim(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        String consumer = readString(in);
        long minIdleMs = readNextLong(in);
        long startMs = readNextLong(in);
        long startSeq = readNextLong(in);
        long count = readNextLong(in);
        // 走 SimpleCache 包装路径以记录 AOF
        if (cache instanceof SimpleCache<?, ?> sc) {
            @SuppressWarnings("unchecked")
            SimpleCache<String, Object> simple = (SimpleCache<String, Object>) sc;
            return simple.xautoclaim(key, group, consumer, minIdleMs, new StreamId(startMs, startSeq), count);
        }
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XAUTOCLAIM 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xautoclaim(key, group, consumer, minIdleMs, new StreamId(startMs, startSeq), count);
    }

    private static StreamGroupsInfo executeXInfoGroups(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XINFO GROUPS 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xinfoGroups(key);
    }

    private static StreamConsumersInfo executeXInfoConsumers(Cache<String, Object> cache, DataInputStream in) throws IOException {
        String key = readString(in);
        String group = readString(in);
        StreamStore store = streamStoreOf(cache);
        if (store == null) {
            throw new RuntimeException("XINFO CONSUMERS 不支持当前 Cache 实现: " + cache.getClass());
        }
        return store.xinfoConsumers(key, group);
    }

    private static boolean readBoolean(DataInputStream in) throws IOException {
        Object obj = Codec.decodeValue(in);
        if (obj instanceof Boolean b) return b;
        if (obj instanceof Number n) return n.intValue() != 0;
        return false;
    }

    // ==================== Stream 命令参数编码（供客户端使用） ====================

    /**
     * 编码 XADD 请求 payload（含 opCode 前缀，可直接传给 {@code CacheClient.send(OP_XADD, payload)}）。
     */
    public static byte[] encodeXAddArgs(String key, StreamId idReq, Map<String, String> fields, XAddOptions options) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XADD);
            out.write(Codec.encodeValue(key));
            long idMs = idReq == null || idReq == StreamId.AUTO ? -1L : idReq.getMsTime();
            long idSeq = idReq == null || idReq == StreamId.AUTO ? -1L : idReq.getSeq();
            out.write(Codec.encodeValue(idMs));
            out.write(Codec.encodeValue(idSeq));
            long maxLen = options != null && options.hasTrim() ? options.getMaxLen() : -1L;
            out.write(Codec.encodeValue(maxLen));
            out.write(Codec.encodeValue(options != null && options.isApproximate()));
            out.write(Codec.encodeValue(options != null && options.isNomkstream()));
            out.writeInt(fields.size());
            for (Map.Entry<String, String> e : fields.entrySet()) {
                out.write(Codec.encodeValue(e.getKey()));
                out.write(Codec.encodeValue(e.getValue()));
            }
        } catch (IOException e) {
            throw new RuntimeException("encode XADD args error", e);
        }
        return bos.toByteArray();
    }

    /**
     * 编码 XRANGE / XREVRANGE 请求 payload（含 opCode 前缀）。
     */
    public static byte[] encodeXRangeArgs(byte opCode, String key, StreamId min, StreamId max, long count) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(opCode);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(min != null ? min.getMsTime() : 0L));
            out.write(Codec.encodeValue(min != null ? min.getSeq() : 0L));
            out.write(Codec.encodeValue(max != null ? max.getMsTime() : Long.MAX_VALUE));
            out.write(Codec.encodeValue(max != null ? max.getSeq() : Long.MAX_VALUE));
            out.write(Codec.encodeValue(count));
        } catch (IOException e) {
            throw new RuntimeException("encode XRANGE args error", e);
        }
        return bos.toByteArray();
    }

    /**
     * 编码 XDEL 请求 payload（含 opCode 前缀）。
     */
    public static byte[] encodeXDelArgs(String key, StreamId... ids) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XDEL);
            out.write(Codec.encodeValue(key));
            out.writeInt(ids == null ? 0 : ids.length);
            if (ids != null) {
                for (StreamId id : ids) {
                    out.write(Codec.encodeValue(id.getMsTime()));
                    out.write(Codec.encodeValue(id.getSeq()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("encode XDEL args error", e);
        }
        return bos.toByteArray();
    }

    /**
     * 编码 XTRIM 请求 payload（含 opCode 前缀）。
     */
    public static byte[] encodeXTrimArgs(String key, long maxLen, boolean approximate) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XTRIM);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(maxLen));
            out.write(Codec.encodeValue(approximate));
        } catch (IOException e) {
            throw new RuntimeException("encode XTRIM args error", e);
        }
        return bos.toByteArray();
    }

    /**
     * 编码 XLEN / XINFO 请求 payload（含 opCode 前缀）。
     */
    public static byte[] encodeKeyOnlyArgs(byte opCode, String key) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(opCode);
            out.write(Codec.encodeValue(key));
        } catch (IOException e) {
            throw new RuntimeException("encode key-only args error", e);
        }
        return bos.toByteArray();
    }

    /**
     * 解码 XADD 响应：返回服务端生成的 StreamId。
     */
    public static StreamId decodeXAddResponse(byte[] payload) {
        Object obj = Codec.decodeValue(payload);
        if (obj instanceof StreamId id) return id;
        if (obj instanceof List<?> list && list.size() == 2) {
            long ms = ((Number) list.getFirst()).longValue();
            long sq = ((Number) list.get(1)).longValue();
            return new StreamId(ms, sq);
        }
        return null;
    }

    /**
     * 解码 XRANGE 响应：返回 StreamEntry 列表。
     */
    @SuppressWarnings("unchecked")
    public static List<StreamEntry> decodeXRangeResponse(byte[] payload) {
        Object obj = Codec.decodeValue(payload);
        if (obj instanceof List<?> list) return (List<StreamEntry>) (List<?>) list;
        return new ArrayList<>();
    }

    // ==================== P1 命令参数编码 ====================

    /** 编码 XREAD 请求 payload。 */
    public static byte[] encodeXReadArgs(String key, StreamId from, long count, long blockMs) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XREAD);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(from != null ? from.getMsTime() : 0L));
            out.write(Codec.encodeValue(from != null ? from.getSeq() : 0L));
            out.write(Codec.encodeValue(count));
            out.write(Codec.encodeValue(blockMs));
        } catch (IOException e) {
            throw new RuntimeException("encode XREAD args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 XGROUP CREATE 请求 payload。 */
    public static byte[] encodeXGroupCreateArgs(String key, String group, StreamId startId, boolean mkstream) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XGROUP_CREATE);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
            out.write(Codec.encodeValue(startId != null ? startId.getMsTime() : 0L));
            out.write(Codec.encodeValue(startId != null ? startId.getSeq() : 0L));
            out.write(Codec.encodeValue(mkstream));
        } catch (IOException e) {
            throw new RuntimeException("encode XGROUP CREATE args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 XGROUP DESTROY 请求 payload。 */
    public static byte[] encodeXGroupDestroyArgs(String key, String group) {
        return encodeKeyGroupArgs(OP_XGROUP_DESTROY, key, group);
    }

    /** 编码 XGROUP CREATECONSUMER 请求 payload。 */
    public static byte[] encodeXGroupCreateConsumerArgs(String key, String group, String consumer) {
        return encodeKeyGroupConsumerArgs(OP_XGROUP_CREATECONSUMER, key, group, consumer);
    }

    /** 编码 XGROUP DELCONSUMER 请求 payload。 */
    public static byte[] encodeXGroupDelConsumerArgs(String key, String group, String consumer) {
        return encodeKeyGroupConsumerArgs(OP_XGROUP_DELCONSUMER, key, group, consumer);
    }

    /** 编码 XREADGROUP 请求 payload。 */
    public static byte[] encodeXReadGroupArgs(String key, String group, String consumer,
                                              boolean readNew, StreamId from, long count, long blockMs) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XREADGROUP);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
            out.write(Codec.encodeValue(consumer));
            out.write(Codec.encodeValue(readNew));
            out.write(Codec.encodeValue(from != null ? from.getMsTime() : 0L));
            out.write(Codec.encodeValue(from != null ? from.getSeq() : 0L));
            out.write(Codec.encodeValue(count));
            out.write(Codec.encodeValue(blockMs));
        } catch (IOException e) {
            throw new RuntimeException("encode XREADGROUP args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 XACK 请求 payload。 */
    public static byte[] encodeXAckArgs(String key, String group, StreamId... ids) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XACK);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
            out.writeInt(ids == null ? 0 : ids.length);
            if (ids != null) {
                for (StreamId id : ids) {
                    out.write(Codec.encodeValue(id.getMsTime()));
                    out.write(Codec.encodeValue(id.getSeq()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("encode XACK args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 XPENDING 请求 payload。consumer 为 null/空表示概要形态。 */
    public static byte[] encodeXPendingArgs(String key, String group,
                                            StreamId start, StreamId end, long count,
                                            String consumer, long minIdleMs) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XPENDING);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
            out.write(Codec.encodeValue(start != null ? start.getMsTime() : 0L));
            out.write(Codec.encodeValue(start != null ? start.getSeq() : 0L));
            out.write(Codec.encodeValue(end != null ? end.getMsTime() : Long.MAX_VALUE));
            out.write(Codec.encodeValue(end != null ? end.getSeq() : Long.MAX_VALUE));
            out.write(Codec.encodeValue(count));
            out.write(Codec.encodeValue(consumer != null ? consumer : ""));
            out.write(Codec.encodeValue(minIdleMs));
        } catch (IOException e) {
            throw new RuntimeException("encode XPENDING args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 XCLAIM 请求 payload。 */
    public static byte[] encodeXClaimArgs(String key, String group, String consumer,
                                          long minIdleMs, StreamId... ids) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XCLAIM);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
            out.write(Codec.encodeValue(consumer));
            out.write(Codec.encodeValue(minIdleMs));
            out.writeInt(ids == null ? 0 : ids.length);
            if (ids != null) {
                for (StreamId id : ids) {
                    out.write(Codec.encodeValue(id.getMsTime()));
                    out.write(Codec.encodeValue(id.getSeq()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("encode XCLAIM args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 XAUTOCLAIM 请求 payload。 */
    public static byte[] encodeXAutoClaimArgs(String key, String group, String consumer,
                                              long minIdleMs, StreamId start, long count) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XAUTOCLAIM);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
            out.write(Codec.encodeValue(consumer));
            out.write(Codec.encodeValue(minIdleMs));
            out.write(Codec.encodeValue(start != null ? start.getMsTime() : 0L));
            out.write(Codec.encodeValue(start != null ? start.getSeq() : 0L));
            out.write(Codec.encodeValue(count));
        } catch (IOException e) {
            throw new RuntimeException("encode XAUTOCLAIM args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 XINFO GROUPS 请求 payload。 */
    public static byte[] encodeXInfoGroupsArgs(String key) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XINFO_GROUPS);
            out.write(Codec.encodeValue(key));
        } catch (IOException e) {
            throw new RuntimeException("encode XINFO GROUPS args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 XINFO CONSUMERS 请求 payload。 */
    public static byte[] encodeXInfoConsumersArgs(String key, String group) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_XINFO_CONSUMERS);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
        } catch (IOException e) {
            throw new RuntimeException("encode XINFO CONSUMERS args error", e);
        }
        return bos.toByteArray();
    }

    private static byte[] encodeKeyGroupArgs(byte opCode, String key, String group) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(opCode);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
        } catch (IOException e) {
            throw new RuntimeException("encode key/group args error", e);
        }
        return bos.toByteArray();
    }

    private static byte[] encodeKeyGroupConsumerArgs(byte opCode, String key, String group, String consumer) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(opCode);
            out.write(Codec.encodeValue(key));
            out.write(Codec.encodeValue(group));
            out.write(Codec.encodeValue(consumer));
        } catch (IOException e) {
            throw new RuntimeException("encode key/group/consumer args error", e);
        }
        return bos.toByteArray();
    }

    // ==================== 集群管理 opCode 编码 ====================

    /**
     * 编码 PING / PONG / GOSSIP 请求 payload。
     * payload 布局：[opCode][senderNodeKey][senderEpoch][membershipSnapshot]
     * <p>membershipSnapshot 为 List&lt;Object&gt;，每个元素为 [host, port, weight, status, epoch, lastHeartbeatMs]。
     */
    public static byte[] encodeGossipArgs(byte opCode, String senderNodeKey, long senderEpoch,
                                          List<List<Object>> membershipSnapshot) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(opCode);
            out.write(Codec.encodeValue(senderNodeKey));
            out.write(Codec.encodeValue(senderEpoch));
            out.write(Codec.encodeValue(membershipSnapshot));
        } catch (IOException e) {
            throw new RuntimeException("encode gossip args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 NODE_JOIN / NODE_LEAVE 请求 payload：[opCode][host][port][weight][epoch] */
    public static byte[] encodeNodeMembershipArgs(byte opCode, String host, int port, double weight, long epoch) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(opCode);
            out.write(Codec.encodeValue(host));
            out.write(Codec.encodeValue(port));
            out.write(Codec.encodeValue(weight));
            out.write(Codec.encodeValue(epoch));
        } catch (IOException e) {
            throw new RuntimeException("encode node membership args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 PROMOTE 请求 payload：[opCode][failedNodeKey][newPrimaryNodeKey][epoch] */
    public static byte[] encodePromoteArgs(String failedNodeKey, String newPrimaryNodeKey, long epoch) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_PROMOTE);
            out.write(Codec.encodeValue(failedNodeKey));
            out.write(Codec.encodeValue(newPrimaryNodeKey));
            out.write(Codec.encodeValue(epoch));
        } catch (IOException e) {
            throw new RuntimeException("encode promote args error", e);
        }
        return bos.toByteArray();
    }

    /**
     * 编码 MIGRATE_PULL 请求 payload：[opCode][ownerHost][ownerPort][keyCount]
     * <p>ownerHost/ownerPort 标识"目标新 primary"的节点；远端节点据此筛选出其作为旧 primary 持有、
     * 但根据新拓扑应迁移到目标节点的 key。
     */
    public static byte[] encodeMigratePullArgs(String targetHost, int targetPort) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_MIGRATE_PULL);
            out.write(Codec.encodeValue(targetHost));
            out.write(Codec.encodeValue(targetPort));
        } catch (IOException e) {
            throw new RuntimeException("encode migrate-pull args error", e);
        }
        return bos.toByteArray();
    }

    /**
     * 编码 MIGRATE_PUSH 请求 payload：[opCode][entries]
     * <p>entries 为 List&lt;Object&gt;，每个元素为 [key, value, remainingTtlMillis]。
     * remainingTtlMillis 为 Long.MAX_VALUE 表示永不过期；-1 表示 key 不存在（被删除）。
     */
    public static byte[] encodeMigratePushArgs(List<Object> entries) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            out.writeByte(OP_MIGRATE_PUSH);
            out.write(Codec.encodeValue(entries));
        } catch (IOException e) {
            throw new RuntimeException("encode migrate-push args error", e);
        }
        return bos.toByteArray();
    }

    /** 编码 CLUSTER_STATUS 请求 payload：[opCode]（无参数） */
    public static byte[] encodeClusterStatusArgs() {
        return new byte[]{OP_CLUSTER_STATUS};
    }
}
