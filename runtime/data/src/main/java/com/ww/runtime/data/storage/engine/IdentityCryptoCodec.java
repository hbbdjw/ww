package com.ww.runtime.data.storage.engine;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * 默认加密实现：透传不加密。保留扩展点，后续接入真实加密算法时替换实现即可。
 */
public class IdentityCryptoCodec implements CryptoCodec {

    @Override
    public boolean enabled() {
        return false;
    }

    @Override
    public OutputStream encryptWrite(OutputStream out, String bucket, String key) {
        return out;
    }

    @Override
    public InputStream decryptRead(InputStream in, String bucket, String key) {
        return in;
    }
}
