package com.ww.runtime.common;

/**
 * 迁移任务错误。
 */
public class MigrationException extends RuntimeFrameworkException {

    public MigrationException(String message) {
        super(message);
    }

    public MigrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
