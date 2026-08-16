package com.ww.runtime.common;

/**
 * 运行时框架错误基类。
 */
public class RuntimeFrameworkException extends RuntimeException {

    public RuntimeFrameworkException(String message) {
        super(message);
    }

    public RuntimeFrameworkException(String message, Throwable cause) {
        super(message, cause);
    }
}
