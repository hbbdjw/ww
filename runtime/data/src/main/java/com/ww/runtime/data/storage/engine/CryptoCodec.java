package com.ww.runtime.data.storage.engine;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * 加解密扩展点（预留）。
 *
 * <p>当前默认实现 {@link IdentityCryptoCodec} 为透传（不加密）。后续可通过实现此接口接入
 * AES-GCM / 服务端加密（SSE-S3 / SSE-C / SSE-KMS）等。ObjectStore 在写入/读取对象数据流时
 * 调用 {@link #encryptWrite} / {@link #decryptRead}，业务层无感知。
 *
 * <p>注意：加密与 read-after-write 一致性正交——加密只改变落盘字节，不改变读写路由与时序。
 */
public interface CryptoCodec {

    /** 是否启用加密（非启用状态下 ObjectStore 可跳过包装以避免无谓拷贝） */
    boolean enabled();

    /**
     * 包装输出流：写入的明文字节经加密后落盘。
     * 调用方负责关闭返回的 OutputStream（同时关闭底层流）。
     */
    OutputStream encryptWrite(OutputStream out, String bucket, String key);

    /**
     * 包装输入流：读取的密文字节经解密后返回明文。
     * 调用方负责关闭返回的 InputStream（同时关闭底层流）。
     */
    InputStream decryptRead(InputStream in, String bucket, String key);
}
