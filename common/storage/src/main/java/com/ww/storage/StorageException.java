package com.ww.storage;

/**
 * 对象存储模块统一异常。继承 RuntimeException 以避免强迫调用方声明 throws。
 */
public class StorageException extends RuntimeException {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
