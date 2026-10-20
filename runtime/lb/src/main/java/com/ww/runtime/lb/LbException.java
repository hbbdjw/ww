package com.ww.runtime.lb;

/**
 * 负载均衡模块统一运行时异常（JDBC 故障、转发失败等）。
 */
public class LbException extends RuntimeException {

    public LbException(String message) {
        super(message);
    }

    public LbException(String message, Throwable cause) {
        super(message, cause);
    }
}
