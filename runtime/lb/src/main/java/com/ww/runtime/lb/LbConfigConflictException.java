package com.ww.runtime.lb;

/**
 * LB 配置乐观锁冲突：更新时携带的 version 与库中当前 version 不一致。
 */
public class LbConfigConflictException extends LbException {

    private final long expectedVersion;
    private final long actualVersion;

    public LbConfigConflictException(long expectedVersion, long actualVersion) {
        super("lb config version conflict: expected=" + expectedVersion + ", actual=" + actualVersion);
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    public long expectedVersion() {
        return expectedVersion;
    }

    public long actualVersion() {
        return actualVersion;
    }
}
