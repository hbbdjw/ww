package com.ww.runtime.data.cache.engine;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
    /** key 字节上限（writeShort 长度字段表达范围），超限拒绝写入而非截断 */
    public static final int MAX_KEY_BYTES = 65535;
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
        if (keyBytes.length > MAX_KEY_BYTES) {
            throw new IOException("AOF key 超过 " + MAX_KEY_BYTES + " 字节上限（实际 "
                    + keyBytes.length + " 字节），拒绝写入");
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

    // ==================== 记录解析 ====================

    /** 结构性损坏：记录边界/长度/编码异常，后续记录不可信（区别于单条毒记录）。 */
    public static class AofFormatException extends IOException {
        public AofFormatException(String message) {
            super(message);
        }
    }

    /** CRC 校验失配：记录体字节在盘上被破坏。 */
    public static final class AofCrcMismatchException extends AofFormatException {
        public AofCrcMismatchException(String message) {
            super(message);
        }
    }

    /** 一条解析完成的 AOF 记录。 */
    public static final class Record {
        private final byte opCode;
        private final String key;
        private final Object[] args;

        Record(byte opCode, String key, Object[] args) {
            this.opCode = opCode;
            this.key = key;
            this.args = args;
        }

        public byte opCode() { return opCode; }
        public String key() { return key; }
        public Object[] args() { return args; }
    }

    /**
     * 边读边累加 CRC 的 tee 流：记录布局无长度前缀，只能在解析过程中捕获原始字节。
     * {@code armed} 关闭时读过的字节不计入 CRC（用于跳过尾部存储的 4 字节 CRC）。
     */
    static final class CrcTeeInputStream extends java.io.FilterInputStream {

        private final java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        private long bytesRead;
        private boolean armed = true;

        CrcTeeInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b >= 0) {
                bytesRead++;
                if (armed) crc.update(b);
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n > 0) {
                bytesRead += n;
                if (armed) crc.update(b, off, n);
            }
            return n;
        }

        void beginRecord() {
            crc.reset();
            armed = true;
        }

        void setArmed(boolean armed) {
            this.armed = armed;
        }

        long crcValue() {
            return crc.getValue();
        }

        /** 累计读过的绝对字节数（含文件头），整条记录成功后即文件内完好边界。 */
        long bytesRead() {
            return bytesRead;
        }
    }

    /**
     * AOF 文件读取器：封装文件头与记录的解析、CRC 校验与偏移追踪。
     *
     * <p>异常语义：
     * <ul>
     *   <li>{@link EOFException}——尾部残记录（崩溃写一半），截断回放；</li>
     *   <li>{@link AofCrcMismatchException}——CRC 失配，截断回放；</li>
     *   <li>{@link AofFormatException}——其他结构性损坏（负长度/解码失败/标记错位），截断回放；</li>
     * </ul>
     * 以上均属结构性损坏，由调用方决定截断；单条毒记录（CRC 完好但应用失败）
     * 不在此处暴露，由回放应用层的 RuntimeException 容错处理。
     */
    public static final class Reader implements java.io.Closeable {

        private final CrcTeeInputStream tee;
        private final DataInputStream in;

        public Reader(InputStream source) {
            this.tee = new CrcTeeInputStream(source);
            this.in = new DataInputStream(tee);
        }

        public int readMagic() throws IOException {
            return in.readInt();
        }

        public int available() throws IOException {
            return in.available();
        }

        /** 当前完好字节边界（每条记录完整读取成功后推进）。 */
        public long bytesRead() {
            return tee.bytesRead();
        }

        public Record readRecord() throws IOException {
            tee.beginRecord();

            byte marker = in.readByte();
            if (marker != CMD_MARKER) {
                throw new AofFormatException("marker 不匹配: 0x" + Integer.toHexString(marker & 0xFF));
            }
            byte opCode = in.readByte();

            int keyLen = in.readUnsignedShort();
            byte[] keyBytes = new byte[keyLen];
            in.readFully(keyBytes);
            String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);

            int argCount = in.readUnsignedShort();
            Object[] args = new Object[argCount];
            for (int i = 0; i < argCount; i++) {
                int argLen = in.readInt();
                if (argLen < 0) {
                    throw new AofFormatException("负参数长度: " + argLen);
                }
                byte[] argBytes = new byte[argLen];
                in.readFully(argBytes);
                try {
                    args[i] = Codec.decodeValue(argBytes);
                } catch (RuntimeException e) {
                    throw new AofFormatException("参数 " + i + " 解码失败: " + e);
                }
            }

            tee.setArmed(false);
            int storedCrc = in.readInt();
            tee.setArmed(true);
            int computedCrc = (int) tee.crcValue();
            if (storedCrc != computedCrc) {
                throw new AofCrcMismatchException(
                        "CRC 失配: op=" + opName(opCode) + ", key=" + key
                                + ", stored=0x" + Integer.toHexString(storedCrc)
                                + ", computed=0x" + Integer.toHexString(computedCrc));
            }
            return new Record(opCode, key, args);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
