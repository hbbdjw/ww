package com.ww.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 自定义二进制值编解码器
 */
final class Codec {

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

    /** P2-8 安全修复：反序列化对象最大字节数（防止恶意大对象 DoS） */
    static final int MAX_OBJECT_SIZE = 16 * 1024 * 1024; // 16MB
    /** P2-8 安全修复：反序列化黑名单（已知的 Java 反序列化 gadget 入口类） */
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

    // ==================== 编码 ====================

    static byte[] encodeValue(Object value) {
        if (value == null) return new byte[]{TYPE_NULL};
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        try {
            encodeValue(out, value);
        } catch (IOException e) {
            throw new RuntimeException("encode error", e);
        }
        return bos.toByteArray();
    }

    private static void encodeValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(TYPE_NULL);
        } else if (value instanceof String) {
            String s = (String) value;
            out.writeByte(TYPE_STRING);
            byte[] utf = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeShort(utf.length);
            out.write(utf);
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
            for (Object item : list) encodeValue(out, item);
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            out.writeByte(TYPE_MAP);
            out.writeInt(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                encodeValue(out, e.getKey());
                encodeValue(out, e.getValue());
            }
        } else if (value instanceof Set) {
            Set<?> set = (Set<?>) value;
            out.writeByte(TYPE_SET);
            out.writeInt(set.size());
            for (Object item : set) encodeValue(out, item);
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
            out.writeShort(utf.length);
            out.write(utf);
        }
    }

    // ==================== 解码 ====================

    static Object decodeValue(byte[] data) {
        if (data == null || data.length == 0) return null;
        try {
            return decodeValue(new DataInputStream(new ByteArrayInputStream(data)));
        } catch (IOException e) {
            throw new RuntimeException("decode error", e);
        }
    }

    static Object decodeValue(DataInputStream in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case TYPE_NULL:
                return null;
            case TYPE_STRING: {
                int len = in.readUnsignedShort();
                byte[] utf = new byte[len];
                in.readFully(utf);
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
                List<Object> list = new ArrayList<Object>(count);
                for (int i = 0; i < count; i++) list.add(decodeValue(in));
                return list;
            }
            case TYPE_MAP: {
                int count = in.readInt();
                if (count < 0) throw new IOException("invalid map size: " + count);
                Map<Object, Object> map = new LinkedHashMap<Object, Object>(count);
                for (int i = 0; i < count; i++) map.put(decodeValue(in), decodeValue(in));
                return map;
            }
            case TYPE_SET: {
                int count = in.readInt();
                if (count < 0) throw new IOException("invalid set size: " + count);
                Set<Object> set = new LinkedHashSet<Object>(count);
                for (int i = 0; i < count; i++) set.add(decodeValue(in));
                return set;
            }
            case TYPE_OBJECT: {
                int len = in.readInt();
                if (len < 0) throw new IOException("invalid object size: " + len);
                // P2-8 安全修复：限制反序列化对象大小
                if (len > MAX_OBJECT_SIZE) {
                    throw new IOException("object size " + len + " exceeds limit " + MAX_OBJECT_SIZE);
                }
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
                try {
                    Object obj = ois.readObject();
                    // P2-8 安全修复：拒绝反序列化已知危险的 gadget 类
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
