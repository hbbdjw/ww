package com.ww.rpc.codec;

/**
 * 序列化接口：将业务对象与 byte[] 相互转换。
 *
 * <p>cluster-rpc 框架层使用 byte[] 作为 payload，不绑定具体序列化方案。
 * 各业务模块可选择 Protobuf/JSON/自定义二进制等方式实现此接口。
 */
public interface Serializer<T> {

    /**
     * 序列化对象为字节数组。
     */
    byte[] serialize(T obj) throws Exception;

    /**
     * 反序列化字节数组为对象。
     */
    T deserialize(byte[] data) throws Exception;
}
