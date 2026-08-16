package com.ww.cache;

/**
 * 缓存模块业务异常。所有修复 P3 引入的统一异常类型，替代裸抛 RuntimeException。
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
