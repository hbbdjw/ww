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
 * CACHE 能力迁移分片编解码：行格式
 * {@code key + 0x01 + remainingTtlMillis(十进制) + 0x01 + base64(valueBytes) + 0x02}（UTF-8），
 * 一条目一分片（与 {@code MockPartitionAdapter} 同口径，保证
 * {@link ChunkCodec#countEntries} 的条目计数与断点偏移语义一致）。
 *
 * <p>值编码（typed，与 common/cache Codec 同族、独立实现避免包私有依赖）：
 * null/String/Integer/Long/Double/Boolean 走类型标记快路径，其余 Serializable
 * 走 ObjectOutputStream。base64 保证字节流不含行分隔符（0x01/0x02）。
 *
 * <p>约束：key 不得含 0x01/0x02 控制字符（与 MockPartitionAdapter 同一层级约束）。
 */
public final class CacheChunkCodec {

    private static final byte TYPE_NULL = 0x00;
    private static final byte TYPE_STRING = 0x01;
    private static final byte TYPE_INT = 0x02;
    private static final byte TYPE_LONG = 0x03;
    private static final byte TYPE_DOUBLE = 0x04;
    private static final byte TYPE_BOOL = 0x05;
    private static final byte TYPE_OBJECT = 0x13;

    private CacheChunkCodec() {
    }

    /** 单条目编码为行格式分片。 */
    public static byte[] encodeEntry(CachePartitionEntry entry) {
        String line = entry.key()
                + Character.toString(ChunkCodec.SEP_KV)
                + entry.remainingTtlMillis()
                + Character.toString(ChunkCodec.SEP_KV)
                + Base64.getEncoder().encodeToString(encodeValue(entry.value()))
                + Character.toString(ChunkCodec.SEP_ENTRY);
        return line.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 行格式反解；任一分隔符缺失/base64 非法/值解码失败返回 null（调用方判 false）。
     */
    public static CachePartitionEntry decodeChunk(byte[] chunk) {
        if (chunk == null || chunk.length == 0) {
            return null;
        }
        int firstSep = indexOf(chunk, ChunkCodec.SEP_KV, 0);
        if (firstSep < 0) {
            return null;
        }
        int secondSep = indexOf(chunk, ChunkCodec.SEP_KV, firstSep + 1);
        if (secondSep < 0) {
            return null;
        }
        int entryEnd = indexOf(chunk, ChunkCodec.SEP_ENTRY, secondSep + 1);
        if (entryEnd != chunk.length - 1) {
            return null; // 必须恰一条目且以终结符收尾
        }
        try {
            String key = new String(chunk, 0, firstSep, StandardCharsets.UTF_8);
            long ttl = Long.parseLong(new String(chunk, firstSep + 1, secondSep - firstSep - 1,
                    StandardCharsets.UTF_8));
            String b64 = new String(chunk, secondSep + 1, entryEnd - secondSep - 1, StandardCharsets.UTF_8);
            Object value = decodeValue(Base64.getDecoder().decode(b64));
            return new CachePartitionEntry(key, ttl, value);
        } catch (RuntimeException e) {
            return null;
        }
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
