package com.ww.runtime.data.cache.engine;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * AOF 二进制格式定义与记录编解码（自 AofService 抽出，R1 第一步）。
 *
 * <p>文件头：4 字节 magic（{@link #AOF_MAGIC}）。
 *
 * <p>记录布局：{@code marker(1) + opCode(1) + keyLen(2) + keyBytes
 * + argCount(2) + [argLen(4) + argBytes]* + crc32(4)}；
 * CRC32 覆盖记录体（不含尾部 4 字节），大端写入。
 *
 * <p>记录解析（readRecord + CRC 校验）与格式版本化在后续阶段补齐。
 */
public final class AofFormat {

    public static final int AOF_MAGIC = 0x414F4631; // "AOF1"
    public static final byte CMD_MARKER = 0x2A; // '*'
    static final String REWRITE_TMP_SUFFIX = ".rewrite-tmp";

    // ==================== 操作类型常量 ====================
    static final byte OP_PUT = 1;
    static final byte OP_PUT_EXPIRED = 2;
    static final byte OP_REMOVE = 3;
    static final byte OP_CLEAR = 4;
    static final byte OP_HSET = 5;
    static final byte OP_LPUSH = 6;
    static final byte OP_RPUSH = 7;
    static final byte OP_LPOP = 8;
    static final byte OP_RPOP = 9;
    static final byte OP_SADD = 10;
    static final byte OP_SREM = 11;
    static final byte OP_ZADD = 12;
    static final byte OP_XADD = 20;
    static final byte OP_XDEL = 21;
    static final byte OP_XTRIM = 22;
    static final byte OP_XGROUP_CREATE = 23;
    static final byte OP_XACK = 24;
    static final byte OP_XGROUP_DESTROY = 25;
    static final byte OP_XGROUP_CREATECONSUMER = 26;
    static final byte OP_XGROUP_DELCONSUMER = 27;
    static final byte OP_XCLAIM = 28;
    static final byte OP_XDELIVER = 29;
    /** AOF 重写专用：恢复单个消费者的 PEL（pending entries list），不记录正常写入流程 */
    static final byte OP_XPEL_ENTRY = 30;

    private AofFormat() {
    }

    static byte opCode(String op) {
        switch (op) {
            case "PUT": return OP_PUT;
            case "PUT_EXPIRED": return OP_PUT_EXPIRED;
            case "REMOVE": return OP_REMOVE;
            case "CLEAR": return OP_CLEAR;
            case "HSET": return OP_HSET;
            case "LPUSH": return OP_LPUSH;
            case "RPUSH": return OP_RPUSH;
            case "LPOP": return OP_LPOP;
            case "RPOP": return OP_RPOP;
            case "SADD": return OP_SADD;
            case "SREM": return OP_SREM;
            case "ZADD": return OP_ZADD;
            case "XADD": return OP_XADD;
            case "XDEL": return OP_XDEL;
            case "XTRIM": return OP_XTRIM;
            case "XGROUP_CREATE": return OP_XGROUP_CREATE;
            case "XACK": return OP_XACK;
            case "XGROUP_DESTROY": return OP_XGROUP_DESTROY;
            case "XGROUP_CREATECONSUMER": return OP_XGROUP_CREATECONSUMER;
            case "XGROUP_DELCONSUMER": return OP_XGROUP_DELCONSUMER;
            case "XCLAIM": return OP_XCLAIM;
            case "XDELIVER": return OP_XDELIVER;
            case "XPEL_ENTRY": return OP_XPEL_ENTRY;
            default: return -1;
        }
    }

    static String opName(byte op) {
        switch (op) {
            case OP_PUT: return "PUT";
            case OP_PUT_EXPIRED: return "PUT_EXPIRED";
            case OP_REMOVE: return "REMOVE";
            case OP_CLEAR: return "CLEAR";
            case OP_HSET: return "HSET";
            case OP_LPUSH: return "LPUSH";
            case OP_RPUSH: return "RPUSH";
            case OP_LPOP: return "LPOP";
            case OP_RPOP: return "RPOP";
            case OP_SADD: return "SADD";
            case OP_SREM: return "SREM";
            case OP_ZADD: return "ZADD";
            case OP_XADD: return "XADD";
            case OP_XDEL: return "XDEL";
            case OP_XTRIM: return "XTRIM";
            case OP_XGROUP_CREATE: return "XGROUP_CREATE";
            case OP_XACK: return "XACK";
            case OP_XGROUP_DESTROY: return "XGROUP_DESTROY";
            case OP_XGROUP_CREATECONSUMER: return "XGROUP_CREATECONSUMER";
            case OP_XGROUP_DELCONSUMER: return "XGROUP_DELCONSUMER";
            case OP_XCLAIM: return "XCLAIM";
            case OP_XDELIVER: return "XDELIVER";
            case OP_XPEL_ENTRY: return "XPEL_ENTRY";
            default: return "UNKNOWN(" + (op & 0xFF) + ")";
        }
    }

    public static byte[] encodeCommand(byte opCode, String key, Object... args) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (keyBytes.length > 65535) {
            keyBytes = Arrays.copyOf(keyBytes, 65535);
        }

        out.writeByte(CMD_MARKER);
        out.writeByte(opCode);
        out.writeShort(keyBytes.length);
        out.write(keyBytes);
        out.writeShort(args != null ? args.length : 0);
        for (Object arg : args) {
            byte[] encoded = Codec.encodeValue(arg);
            out.writeInt(encoded.length);
            out.write(encoded);
        }

        out.flush();
        byte[] data = bos.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(data);
        int crcValue = (int) crc.getValue();
        // 直接追加 CRC 4 字节（大端，与 DataOutputStream.writeInt 一致），避免第二次 toByteArray 全量拷贝
        byte[] result = Arrays.copyOf(data, data.length + 4);
        result[data.length] = (byte) (crcValue >>> 24);
        result[data.length + 1] = (byte) (crcValue >>> 16);
        result[data.length + 2] = (byte) (crcValue >>> 8);
        result[data.length + 3] = (byte) crcValue;
        return result;
    }
}
