package com.ww.runtime.data.cache.engine;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;

/**
 * AOF 二进制格式定义与记录编解码（自 AofService 抽出，R1 第一步；
 * 阶段 3 承载格式版本化 R7：全部新旧格式分支集中在本类与 {@link Codec}）。
 *
 * <p>文件头：
 * <ul>
 *   <li>v1：4 字节 magic（{@link #AOF_MAGIC}，"AOF1"）——存量文件；</li>
 *   <li>v2：magic（{@link #AOF_MAGIC_V2}，"AOF2"）+ 4 字节版本号（{@link #FORMAT_VERSION_V2}）。</li>
 * </ul>
 *
 * <p>记录布局（v1/v2 相同）：{@code marker(1) + opCode(1) + keyLen(2) + keyBytes
 * + argCount(2) + [argLen(4) + argBytes]* + crc32(4)}；
 * CRC32 覆盖记录体（不含尾部 4 字节），大端写入。
 * 记录内 value 编解码（{@link Codec}）按版本分支：v1 字符串长度 short（截断隐患），
 * v2 为 int（C13）。
 *
 * <p>v2 新增操作码（§3.4）：{@link #OP_PUT_EXPIRED_ABS}（绝对过期时间戳，C6 消灭重启续期）、
 * {@link #OP_EXPIRE}（容器 TTL 持久化，C7）。
 */
public final class AofFormat {

    public static final int AOF_MAGIC = 0x414F4631; // "AOF1"
    public static final int AOF_MAGIC_V2 = 0x414F4632; // "AOF2"
    public static final int FORMAT_VERSION_V1 = 1;
    public static final int FORMAT_VERSION_V2 = 2;
    /** 新建文件/重写产物使用的当前版本 */
    public static final int FORMAT_VERSION_LATEST = FORMAT_VERSION_V2;
    public static final byte CMD_MARKER = 0x2A; // '*'
    /** key 字节上限（writeShort 长度字段表达范围），超限拒绝写入而非截断 */
    public static final int MAX_KEY_BYTES = 65535;
    /** key 非法控制字符（迁移分片编码以 0x01/0x02 作分隔符，含之的 key 无法安全迁移） */
    public static final byte FORBIDDEN_KEY_BYTE_1 = 0x01;
    public static final byte FORBIDDEN_KEY_BYTE_2 = 0x02;
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
    /** v2：PUT 携带绝对过期时间戳（C6，回放不再续期） */
    static final byte OP_PUT_EXPIRED_ABS = 31;
    /** v2：容器 TTL（C7，参数为绝对过期时间戳；<=0/超限语义见回放分支） */
    static final byte OP_EXPIRE = 32;
    /** v2：迁移导入 Stream 批量追加（C29），args=[flatIds(List<Long>), fieldsList(List<Map>)] */
    static final byte OP_XADD_BULK = 33;
    /** v2：批量删除（C30 清扫批量化），args=[keys(List<String>)] */
    static final byte OP_REMOVE_MULTI = 34;
    /** HDEL：args=[fields(List<String>)]；删空后回放同样移除 key。 */
    static final byte OP_HDEL = 35;
    /** ZREM：args=[members(List<Object>)]；删空后回放同样移除 key。 */
    static final byte OP_ZREM = 36;

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
            case "HDEL": return OP_HDEL;
            case "ZREM": return OP_ZREM;
            case "EXPIRE": return OP_EXPIRE;
            case "XADD_BULK": return OP_XADD_BULK;
            case "REMOVE_MULTI": return OP_REMOVE_MULTI;
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
            case OP_HDEL: return "HDEL";
            case OP_ZREM: return "ZREM";
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
            case OP_PUT_EXPIRED_ABS: return "PUT_EXPIRED_ABS";
            case OP_EXPIRE: return "EXPIRE";
            case OP_XADD_BULK: return "XADD_BULK";
            case OP_REMOVE_MULTI: return "REMOVE_MULTI";
            default: return "UNKNOWN(" + (op & 0xFF) + ")";
        }
    }

    /** C43：每线程复用的编码 sink（BAOS+DataOutputStream+CRC32），消灭每命令多次对象分配。 */
    private static final ThreadLocal<RecordSink> TL_SINK = ThreadLocal.withInitial(RecordSink::new);

    private static final class ExposedBos extends java.io.ByteArrayOutputStream {
        byte[] bufRef() {
            return buf; // protected 字段：同包子类可见
        }
    }

    private static final class RecordSink {
        final ExposedBos bos = new ExposedBos();
        final DataOutputStream out = new DataOutputStream(bos);
        final CRC32 crc = new CRC32();
    }

    public static byte[] encodeCommand(byte opCode, String key, Object... args) throws IOException {
        return encodeCommand(FORMAT_VERSION_LATEST, opCode, key, args);
    }

    /** 版本化编码：记录布局 v1/v2 相同，value 编码（Codec 字符串长度前缀）按版本分支。 */
    public static byte[] encodeCommand(int formatVersion, byte opCode, String key, Object... args)
            throws IOException {
        // C43：线程内复用缓冲，CRC 直接在内部数组上计算，全链路单次 toByteArray 拷贝
        RecordSink sink = TL_SINK.get();
        sink.bos.reset();
        DataOutputStream out = sink.out;

        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (keyBytes.length > MAX_KEY_BYTES) {
            throw new IOException("AOF key 超过 " + MAX_KEY_BYTES + " 字节上限（实际 "
                    + keyBytes.length + " 字节），拒绝写入");
        }
        // C41：0x01/0x02 为迁移分片编码分隔符，含之的 key 迁移解码必失败，写入前拒绝
        for (byte b : keyBytes) {
            if (b == FORBIDDEN_KEY_BYTE_1 || b == FORBIDDEN_KEY_BYTE_2) {
                throw new IOException(String.format(
                        "AOF key 含非法控制符 0x0%s，拒绝写入（C41）",
                        b == FORBIDDEN_KEY_BYTE_1 ? "1" : "2"));
            }
        }

        out.writeByte(CMD_MARKER);
        out.writeByte(opCode);
        out.writeShort(keyBytes.length);
        out.write(keyBytes);
        out.writeShort(args != null ? args.length : 0);
        for (Object arg : args) {
            byte[] encoded = Codec.encodeValue(arg, formatVersion);
            out.writeInt(encoded.length);
            out.write(encoded);
        }

        int size = sink.bos.size();
        sink.crc.reset();
        sink.crc.update(sink.bos.bufRef(), 0, size);
        int crcValue = (int) sink.crc.getValue();
        // CRC 大端直接追加（DataOutputStream 逐字节 write 与 writeInt 字节序一致）
        out.writeByte(crcValue >>> 24);
        out.writeByte(crcValue >>> 16);
        out.writeByte(crcValue >>> 8);
        out.writeByte(crcValue);
        return sink.bos.toByteArray();
    }

    // ==================== 文件头 ====================

    /** 写文件头：v1 = magic；v2 = magic + 版本号。 */
    public static void writeHeader(DataOutputStream out, int formatVersion) throws IOException {
        out.writeInt(formatVersion >= FORMAT_VERSION_V2 ? AOF_MAGIC_V2 : AOF_MAGIC);
        if (formatVersion >= FORMAT_VERSION_V2) {
            out.writeInt(formatVersion);
        }
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
        /** 文件格式版本（决定 value 解码分支：v1 字符串 short 长度，v2 int）；魔数解析后可修正 */
        private volatile int formatVersion;

        public Reader(InputStream source) {
            this(source, FORMAT_VERSION_LATEST);
        }

        public Reader(InputStream source, int formatVersion) {
            this.tee = new CrcTeeInputStream(source);
            this.in = new DataInputStream(tee);
            this.formatVersion = formatVersion;
        }

        /** 魔数解析后修正版本（包内可见：回放器先读头再定版本）。 */
        void setFormatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
        }

        public int formatVersion() {
            return formatVersion;
        }

        public int readMagic() throws IOException {
            return in.readInt();
        }

        /** 读取 v2 文件头的版本号（紧跟 magic 之后；仅 v2 文件调用）。 */
        public int readVersion() throws IOException {
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
                    args[i] = Codec.decodeValue(argBytes, formatVersion);
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
