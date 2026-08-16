package com.ww.runtime.common;

/**
 * 租约获取失败。
 */
public class LeaseAcquireException extends RuntimeFrameworkException {

    public LeaseAcquireException(String message) {
        super(message);
    }

    public LeaseAcquireException(String message, Throwable cause) {
        super(message, cause);
    }
}
