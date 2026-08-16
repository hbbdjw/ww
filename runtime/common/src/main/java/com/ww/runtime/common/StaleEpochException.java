package com.ww.runtime.common;

/**
 * 过期纪元：携带消息的纪元落后于当前纪元时抛出。
 */
public class StaleEpochException extends RuntimeFrameworkException {

    private final long expectedEpoch;
    private final long actualEpoch;

    public StaleEpochException(long expectedEpoch, long actualEpoch) {
        super("stale epoch: expected >= " + expectedEpoch + ", but got " + actualEpoch);
        this.expectedEpoch = expectedEpoch;
        this.actualEpoch = actualEpoch;
    }

    public StaleEpochException(String message) {
        super(message);
        this.expectedEpoch = -1;
        this.actualEpoch = -1;
    }

    public long expectedEpoch() {
        return expectedEpoch;
    }

    public long actualEpoch() {
        return actualEpoch;
    }
}
