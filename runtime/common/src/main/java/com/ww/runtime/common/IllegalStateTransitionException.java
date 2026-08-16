package com.ww.runtime.common;

/**
 * 非法状态迁移：状态机收到不允许的 from -> to 迁移时抛出。
 */
public class IllegalStateTransitionException extends RuntimeFrameworkException {

    private final String from;
    private final String to;

    public IllegalStateTransitionException(Object from, Object to) {
        super("illegal state transition: " + from + " -> " + to);
        this.from = String.valueOf(from);
        this.to = String.valueOf(to);
    }

    public IllegalStateTransitionException(Object from, Object to, String detail) {
        super("illegal state transition: " + from + " -> " + to
                + (detail == null || detail.isBlank() ? "" : " (" + detail + ")"));
        this.from = String.valueOf(from);
        this.to = String.valueOf(to);
    }

    public String from() {
        return from;
    }

    public String to() {
        return to;
    }
}
