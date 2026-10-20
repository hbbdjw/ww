package com.ww.runtime.data.cache.engine;

/**
 * 缓存模块统一业务异常类型。
 * 继承 RuntimeException 以保持向后兼容（不强迫现有调用方增加 throws 声明）。
 */
public class CacheException extends RuntimeException {

    public CacheException(String message) {
        super(message);
    }

    public CacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
