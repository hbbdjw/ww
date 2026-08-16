package com.ww.rpc.codec;

/**
 * 字节数组透传序列化器：payload 已经是 byte[] 时直接使用，无需额外转换。
 */
public class ByteArraySerializer implements Serializer<byte[]> {

    public static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

    @Override
    public byte[] serialize(byte[] obj) {
        return obj == null ? new byte[0] : obj;
    }

    @Override
    public byte[] deserialize(byte[] data) {
        return data == null ? new byte[0] : data;
    }
}
