package com.ww.runtime.data.cache.engine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义二进制值编解码器（对网关转发层开放 encode/decode 入口）。
 *
 * <p>格式版本化（R7/C13，阶段 3）：字符串长度前缀 v1 为 short（&gt;64KB 静默截断损坏），
 * v2 起为 int。运行期编码一律 v2；旧格式解码分支集中在本类
 * （{@link #decodeValue(byte[], int)}），仅供 AOF v1 文件回放使用。
 */
public final class Codec {

    private static final Logger FILTER_LOG = LoggerFactory.getLogger(Codec.class);

    static final byte TYPE_NULL    = 0x00;
    static final byte TYPE_STRING  = 0x01;
    static final byte TYPE_INT     = 0x02;
    static final byte TYPE_LONG    = 0x03;
    static final byte TYPE_DOUBLE  = 0x04;
    static final byte TYPE_BOOL    = 0x05;
    static final byte TYPE_LIST    = 0x10;
    static final byte TYPE_MAP     = 0x11;
    static final byte TYPE_SET     = 0x12;
    static final byte TYPE_OBJECT  = 0x13;

    /** 反序列化对象最大字节数（防止恶意大对象 DoS） */
    static final int MAX_OBJECT_SIZE = 16 * 1024 * 1024; // 16MB
    /** 反序列化黑名单（已知的 Java 反序列化 gadget 入口类） */
    static final java.util.Set<String> BLOCKED_CLASSES = java.util.Set.of(
            "org.apache.commons.collections.functors.InvokerTransformer",
            "org.apache.commons.collections.functors.InstantiateTransformer",
            "org.apache.commons.collections4.functors.InvokerTransformer",
            "org.apache.commons.collections4.functors.InstantiateTransformer",
            "org.apache.xalan.xsltc.trax.TemplatesImpl",
            "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl",
            "javax.management.loading.MLet",
            "java.net.URLClassLoader"
    );

    private Codec() {}

    // ==================== 反序列化白名单过滤（C14，JEP 290） ====================

    /**
     * C14 第一步（dry-run）：ObjectInputFilter 白名单，默认 dry-run 模式——
     * 越界类仅记日志+计数、不拒绝，收集真实流量中的业务类清单；观察期后由
     * {@link #setObjectFilterEnforcing(boolean)} 切换为拒绝模式。
     * <p>白名单：JDK 基础包（java/javax/jdk/sun）、本工程包（com.ww.**）、数组（"[L" 前缀）、
     * 基本类型。
     */
    private static final java.util.function.Predicate<Class<?>> DESERIALIZATION_WHITELIST = clazz -> {
        if (clazz == null) {
            return true;
        }
        if (clazz.isPrimitive()) {
            return true;
        }
        String name = clazz.getName();
        if (name.startsWith("[")) {
            return true; // 数组类型，元素类由递归过滤覆盖
        }
        return name.startsWith("java.") || name.startsWith("javax.")
                || name.startsWith("jdk.") || name.startsWith("sun.")
                || name.startsWith("com.ww.");
    };

    /** C14 过滤模式：false=dry-run（默认，仅日志），true=拒绝 */
    private static volatile boolean objectFilterEnforcing = false;
    /** dry-run 期间发现的越界反序列化类次数（可观测性：切换拒绝模式前须清零确认） */
    private static final java.util.concurrent.atomic.AtomicLong objectFilterViolations =
            new java.util.concurrent.atomic.AtomicLong();

    /** 切换 C14 过滤模式（true=拒绝越界类，false=dry-run 仅告警）。 */
    public static void setObjectFilterEnforcing(boolean enforcing) {
        objectFilterEnforcing = enforcing;
        FILTER_LOG.info("C14 反序列化白名单过滤模式切换：{}", enforcing ? "拒绝" : "dry-run（仅告警）");
    }

    /** dry-run 期间发现的越界反序列化类次数。 */
    public static long getObjectFilterViolations() {
        return objectFilterViolations.get();
    }

    // ==================== 编码 ====================

    public static byte[] encodeValue(Object value) {
        return encodeValue(value, AofFormat.FORMAT_VERSION_LATEST);
    }

    public static byte[] encodeValue(Object value, int formatVersion) {
        if (value == null) return new byte[]{TYPE_NULL};
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            encodeValue(out, value, formatVersion);
        } catch (IOException e) {
            throw new RuntimeException("encode error", e);
        }
        return bos.toByteArray();
    }

    private static void encodeValue(DataOutputStream out, Object value, int formatVersion) throws IOException {
        if (value == null) {
            out.writeByte(TYPE_NULL);
        } else if (value instanceof String) {
            String s = (String) value;
            out.writeByte(TYPE_STRING);
            byte[] utf = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            writeSizedBytes(out, utf, formatVersion);
        } else if (value instanceof Integer) {
            Integer i = (Integer) value;
            out.writeByte(TYPE_INT);
            out.writeInt(i);
        } else if (value instanceof Long) {
            Long l = (Long) value;
            out.writeByte(TYPE_LONG);
            out.writeLong(l);
        } else if (value instanceof Double) {
            Double d = (Double) value;
            out.writeByte(TYPE_DOUBLE);
            out.writeLong(Double.doubleToRawLongBits(d));
        } else if (value instanceof Boolean) {
            Boolean b = (Boolean) value;
            out.writeByte(TYPE_BOOL);
            out.writeBoolean(b);
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            out.writeByte(TYPE_LIST);
            out.writeInt(list.size());
            for (Object item : list) encodeValue(out, item, formatVersion);
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            out.writeByte(TYPE_MAP);
            out.writeInt(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                encodeValue(out, e.getKey(), formatVersion);
                encodeValue(out, e.getValue(), formatVersion);
            }
        } else if (value instanceof Set) {
            Set<?> set = (Set<?>) value;
            out.writeByte(TYPE_SET);
            out.writeInt(set.size());
            for (Object item : set) encodeValue(out, item, formatVersion);
        } else if (value instanceof Serializable) {
            out.writeByte(TYPE_OBJECT);
            ByteArrayOutputStream objBos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(objBos);
            try {
                oos.writeObject(value);
            } finally {
                oos.close();
            }
            byte[] bytes = objBos.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        } else {
            out.writeByte(TYPE_STRING);
            byte[] utf = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            writeSizedBytes(out, utf, formatVersion);
        }
    }

    /**
     * C13：版本化字符串长度前缀。v1 的 writeShort 超 64KB 静默截断损坏；
     * v2 改 int 长度前缀。v1 路径仅在 v1 文件续写时使用，超限响亮失败
     * （对齐 CacheChunkCodec.writeUTF 语义）。
     */
    private static void writeSizedBytes(DataOutputStream out, byte[] utf, int formatVersion) throws IOException {
        if (formatVersion <= AofFormat.FORMAT_VERSION_V1) {
            if (utf.length > 0xFFFF) {
                throw new IOException("v1 字符串编码超长（" + utf.length
                        + " 字节 > 65535），拒绝写入（C13）");
            }
            out.writeShort(utf.length);
        } else {
            out.writeInt(utf.length);
        }
        out.write(utf);
    }

    // ==================== 解码 ====================

    public static Object decodeValue(byte[] data) {
        return decodeValue(data, AofFormat.FORMAT_VERSION_LATEST);
    }

    /** 版本化解码入口：旧格式分支集中于此（R7）。v1 字符串长度为 short，v2 起为 int。 */
    public static Object decodeValue(byte[] data, int formatVersion) {
        if (data == null || data.length == 0) return null;
        try {
            return decodeValue(new DataInputStream(new ByteArrayInputStream(data)), formatVersion);
        } catch (IOException e) {
            throw new RuntimeException("decode error", e);
        }
    }

    static Object decodeValue(DataInputStream in) throws IOException {
        return decodeValue(in, AofFormat.FORMAT_VERSION_LATEST);
    }

    static Object decodeValue(DataInputStream in, int formatVersion) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case TYPE_NULL:
                return null;
            case TYPE_STRING: {
                byte[] utf = readSizedBytes(in, formatVersion);
                return new String(utf, java.nio.charset.StandardCharsets.UTF_8);
            }
            case TYPE_INT:
                return in.readInt();
            case TYPE_LONG:
                return in.readLong();
            case TYPE_DOUBLE:
                return Double.longBitsToDouble(in.readLong());
            case TYPE_BOOL:
                return in.readBoolean();
            case TYPE_LIST: {
                int count = in.readInt();
                if (count < 0) throw new IOException("invalid list size: " + count);
                // 预分配钳制 ≤ 剩余字节数：损坏的巨大 count 不得直接 OOM（decodeValue(byte[])
                // 底层为 ByteArrayInputStream，available 精确）；真实条数超限由后续读取 EOF 暴露
                List<Object> list = new ArrayList<Object>(Math.min(count, in.available()));
                for (int i = 0; i < count; i++) list.add(decodeValue(in, formatVersion));
                return list;
            }
            case TYPE_MAP: {
                int count = in.readInt();
                if (count < 0) throw new IOException("invalid map size: " + count);
                Map<Object, Object> map = new LinkedHashMap<Object, Object>(Math.min(count, in.available()));
                for (int i = 0; i < count; i++) map.put(decodeValue(in, formatVersion), decodeValue(in, formatVersion));
                return map;
            }
            case TYPE_SET: {
                int count = in.readInt();
                if (count < 0) throw new IOException("invalid set size: " + count);
                Set<Object> set = new LinkedHashSet<Object>(Math.min(count, in.available()));
                for (int i = 0; i < count; i++) set.add(decodeValue(in, formatVersion));
                return set;
            }
            case TYPE_OBJECT: {
                int len = in.readInt();
                if (len < 0) throw new IOException("invalid object size: " + len);
                // 限制反序列化对象大小
                if (len > MAX_OBJECT_SIZE) {
                    throw new IOException("object size " + len + " exceeds limit " + MAX_OBJECT_SIZE);
                }
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
                // C14：JEP 290 白名单过滤在 readObject（gadget 链执行）之前生效。
                // dry-run 模式仅记录越界类不拒绝；拒绝模式抛异常中止反序列化
                ois.setObjectInputFilter(info -> {
                    Class<?> serialClass = info.serialClass();
                    if (serialClass == null) {
                        // 引用/数组深度等非类信息，交给默认深度与大小检查
                        return ObjectInputFilter.Status.ALLOWED;
                    }
                    if (DESERIALIZATION_WHITELIST.test(serialClass)) {
                        return ObjectInputFilter.Status.ALLOWED;
                    }
                    String name = serialClass.getName();
                    if (objectFilterEnforcing) {
                        FILTER_LOG.warn("C14 拒绝反序列化白名单外类：{}", name);
                        return ObjectInputFilter.Status.REJECTED;
                    }
                    objectFilterViolations.incrementAndGet();
                    FILTER_LOG.warn("C14 dry-run：白名单外反序列化类（当前仅告警不拒绝）：{}", name);
                    return ObjectInputFilter.Status.ALLOWED;
                });
                try {
                    Object obj = ois.readObject();
                    // 拒绝反序列化已知危险的 gadget 类
                    if (obj != null && BLOCKED_CLASSES.contains(obj.getClass().getName())) {
                        throw new IOException("blocked deserialization of class: " + obj.getClass().getName());
                    }
                    return obj;
                } catch (ClassNotFoundException e) {
                    throw new IOException("deserialize object failed: class not found", e);
                } finally {
                    ois.close();
                }
            }
            default:
                throw new IOException("unknown type tag: " + Integer.toHexString(type & 0xFF));
        }
    }

    /** 版本化长度前缀读取：v1 为 unsignedShort（兼容存量），v2 起为 int。 */
    private static byte[] readSizedBytes(DataInputStream in, int formatVersion) throws IOException {
        int len = formatVersion <= AofFormat.FORMAT_VERSION_V1
                ? in.readUnsignedShort()
                : in.readInt();
        if (len < 0) {
            throw new IOException("negative string length: " + len);
        }
        byte[] utf = new byte[len];
        in.readFully(utf);
        return utf;
    }

    static <T> T decodeValue(byte[] data, Class<T> type) {
        Object obj = decodeValue(data);
        if (type.isInstance(obj)) return type.cast(obj);
        return null;
    }

    static <T> T decodeValue(DataInputStream in, Class<T> type) throws IOException {
        Object obj = decodeValue(in);
        if (type.isInstance(obj)) return type.cast(obj);
        return null;
    }
}
