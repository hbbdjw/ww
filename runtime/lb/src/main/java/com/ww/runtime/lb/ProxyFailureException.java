package com.ww.runtime.lb;

/**
 * 反代请求失败。{@link #retryable()} 为 true 表示连接/超时类失败，
 * 仅允许对 GET/HEAD 换另一健康节点重试一次；写方法一律不重试。
 */
public class ProxyFailureException extends LbException {

    private final boolean retryable;

    public ProxyFailureException(String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean retryable() {
        return retryable;
    }
}
