package com.ww.runtime.data.cache;

import com.ww.runtime.migration.ChunkCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * CACHE 能力迁移分片编解码（v2 全类型行格式）：
 * {@code key + 0x01 + kind + 0x01 + ttl(十进制) + 0x01 + base64(payloadBytes) + 0x02}（UTF-8），
 * 一条目一分片（与 {@code MockPartitionAdapter} 同口径，保证
 * {@link ChunkCodec#countEntries} 的条目计数与断点偏移语义一致）。
 *
 * <p>载荷编码（typed）：KV 沿用 {@link #encodeValue} 的类型标记快路径
 * （null/String/Integer/Long/Double/Boolean，其余 Serializable 走
 * ObjectOutputStream）；HASH/LIST/SET/ZSET/STREAM 载荷整体走
 * ObjectOutputStream（base64 保证字节流不含行分隔符 0x01/0x02）。</p>
 *
 * <p>约束：key 不得含 0x01/0x02 控制字符（与 MockPartitionAdapter 同一层级约束）。
 * 不兼容 v1 行格式（key + ttl + value）——升级须等在途迁移任务清空（见计划文档）。</p>
 */
public final class CacheChunkCodec {

    private static final byte TYPE_NULL = 0x00;
    private static final byte TYPE_STRING = 0x01;
    private static final byte TYPE_INT = 0x02;
    private static final byte TYPE_LONG = 0x03;
    private static final byte TYPE_DOUBLE = 0x04;
    private static final byte TYPE_BOOL = 0x05;
    private static final byte TYPE_OBJECT = 0x13;

    /** kind 与行格式标记的互转（单字符）。 */
    private static final String KIND_KV = "K";
    private static final String KIND_HASH = "H";
    private static final String KIND_LIST = "L";
    private static final String KIND_SET = "S";
    private static final String KIND_ZSET = "Z";
    private static final String KIND_STREAM = "T";

    private CacheChunkCodec() {
    }

    /** 单条目编码为行格式分片。 */
    public static byte[] encodeEntry(CachePartitionEntry entry) {
        byte[] payloadBytes = encodePayload(entry);
        String line = entry.key()
                + Character.toString(ChunkCodec.SEP_KV)
                + kindTag(entry.kind())
                + Character.toString(ChunkCodec.SEP_KV)
                + entry.remainingTtlMillis()
                + Character.toString(ChunkCodec.SEP_KV)
                + Base64.getEncoder().encodeToString(payloadBytes)
                + Character.toString(ChunkCodec.SEP_ENTRY);
        return line.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 行格式反解；任一分隔符缺失/kind 未知/base64 非法/载荷解码失败返回 null（调用方判 false）。
     */
    public static CachePartitionEntry decodeChunk(byte[] chunk) {
        if (chunk == null || chunk.length == 0) {
            return null;
        }
        int sep1 = indexOf(chunk, ChunkCodec.SEP_KV, 0);
        if (sep1 < 0) {
            return null;
        }
        int sep2 = indexOf(chunk, ChunkCodec.SEP_KV, sep1 + 1);
        if (sep2 < 0) {
            return null;
        }
        int sep3 = indexOf(chunk, ChunkCodec.SEP_KV, sep2 + 1);
        if (sep3 < 0) {
            return null;
        }
        int entryEnd = indexOf(chunk, ChunkCodec.SEP_ENTRY, sep3 + 1);
        if (entryEnd != chunk.length - 1) {
            return null; // 必须恰一条目且以终结符收尾
        }
        try {
            String key = new String(chunk, 0, sep1, StandardCharsets.UTF_8);
            CachePartitionEntry.Kind kind = parseKindTag(
                    new String(chunk, sep1 + 1, sep2 - sep1 - 1, StandardCharsets.UTF_8));
            long ttl = Long.parseLong(new String(chunk, sep2 + 1, sep3 - sep2 - 1,
                    StandardCharsets.UTF_8));
            String b64 = new String(chunk, sep3 + 1, entryEnd - sep3 - 1, StandardCharsets.UTF_8);
            Object payload = decodePayload(kind, Base64.getDecoder().decode(b64));
            return new CachePartitionEntry(key, kind, ttl, payload);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** 载荷编码：KV 走类型化快路径；容器整体走 ObjectOutputStream。 */
    private static byte[] encodePayload(CachePartitionEntry entry) {
        if (entry.kind() == CachePartitionEntry.Kind.KV) {
            return encodeValue(entry.payload());
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(entry.payload());
        } catch (IOException e) {
            throw new IllegalArgumentException("cache payload encode failed", e);
        }
        return bos.toByteArray();
    }

    /** 载荷解码：KV 走类型化快路径；容器经 ObjectInputStream 还原。 */
    private static Object decodePayload(CachePartitionEntry.Kind kind, byte[] bytes) {
        if (kind == CachePartitionEntry.Kind.KV) {
            return decodeValue(bytes);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalArgumentException("cache payload decode failed", e);
        }
    }

    private static String kindTag(CachePartitionEntry.Kind kind) {
        return switch (kind) {
            case KV -> KIND_KV;
            case HASH -> KIND_HASH;
            case LIST -> KIND_LIST;
            case SET -> KIND_SET;
            case ZSET -> KIND_ZSET;
            case STREAM -> KIND_STREAM;
        };
    }

    private static CachePartitionEntry.Kind parseKindTag(String tag) {
        return switch (tag) {
            case KIND_KV -> CachePartitionEntry.Kind.KV;
            case KIND_HASH -> CachePartitionEntry.Kind.HASH;
            case KIND_LIST -> CachePartitionEntry.Kind.LIST;
            case KIND_SET -> CachePartitionEntry.Kind.SET;
            case KIND_ZSET -> CachePartitionEntry.Kind.ZSET;
            case KIND_STREAM -> CachePartitionEntry.Kind.STREAM;
            default -> throw new IllegalArgumentException("unknown kind tag: " + tag);
        };
    }

    // ------------------------------------------------------------------
    // 键型断点游标（迁移并发写入场景：位置断点会因字典序插入/删除而错位）
    // ------------------------------------------------------------------

    /** 从单条目分片提取游标（kind 序号 + Base64(key)）；格式非法返回 null。 */
    public static String cursorOf(byte[] chunk) {
        if (chunk == null || chunk.length == 0) {
            return null;
        }
        int sep1 = indexOf(chunk, ChunkCodec.SEP_KV, 0);
        int sep2 = indexOf(chunk, ChunkCodec.SEP_KV, sep1 + 1);
        if (sep1 < 0 || sep2 < 0) {
            return null;
        }
        try {
            String key = new String(chunk, 0, sep1, StandardCharsets.UTF_8);
            CachePartitionEntry.Kind kind = parseKindTag(
                    new String(chunk, sep1 + 1, sep2 - sep1 - 1, StandardCharsets.UTF_8));
            return kind.ordinal() + "|" + Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** 条目是否严格位于游标之后（与导出排序 key → kind.ordinal 完全一致）。 */
    public static boolean isAfterCursor(CachePartitionEntry entry, String cursor) {
        if (cursor == null) {
            return true;
        }
        int bar = cursor.indexOf('|');
        if (bar <= 0) {
            throw new IllegalArgumentException("invalid cache migration cursor: " + cursor);
        }
        int cursorKind = Integer.parseInt(cursor.substring(0, bar));
        String cursorKey = new String(Base64.getUrlDecoder().decode(cursor.substring(bar + 1)),
                StandardCharsets.UTF_8);
        int keyCompare = entry.key().compareTo(cursorKey);
        return keyCompare > 0 || (keyCompare == 0 && entry.kind().ordinal() > cursorKind);
    }

    /** 值类型化编码：基础类型快路径 + Serializable 兜底。 */
    static byte[] encodeValue(Object value) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            if (value == null) {
                out.writeByte(TYPE_NULL);
            } else if (value instanceof String s) {
                out.writeByte(TYPE_STRING);
                out.writeUTF(s);
            } else if (value instanceof Integer i) {
                out.writeByte(TYPE_INT);
                out.writeInt(i);
            } else if (value instanceof Long l) {
                out.writeByte(TYPE_LONG);
                out.writeLong(l);
            } else if (value instanceof Double d) {
                out.writeByte(TYPE_DOUBLE);
                out.writeDouble(d);
            } else if (value instanceof Boolean b) {
                out.writeByte(TYPE_BOOL);
                out.writeBoolean(b);
            } else {
                out.writeByte(TYPE_OBJECT);
                ObjectOutputStream oos = new ObjectOutputStream(out);
                oos.writeObject(value);
                oos.flush();
            }
            out.flush();
        } catch (IOException e) {
            throw new IllegalArgumentException("cache value encode failed", e);
        }
        return bos.toByteArray();
    }

    /** 值类型化反解。 */
    static Object decodeValue(byte[] bytes) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            byte type = in.readByte();
            return switch (type) {
                case TYPE_NULL -> null;
                case TYPE_STRING -> in.readUTF();
                case TYPE_INT -> in.readInt();
                case TYPE_LONG -> in.readLong();
                case TYPE_DOUBLE -> in.readDouble();
                case TYPE_BOOL -> in.readBoolean();
                case TYPE_OBJECT -> new ObjectInputStream(in).readObject();
                default -> throw new IllegalArgumentException("unknown value type: " + type);
            };
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalArgumentException("cache value decode failed", e);
        }
    }

    private static int indexOf(byte[] haystack, byte needle, int from) {
        for (int i = from; i < haystack.length; i++) {
            if (haystack[i] == needle) {
                return i;
            }
        }
        return -1;
    }
}
