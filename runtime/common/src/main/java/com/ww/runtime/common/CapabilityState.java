package com.ww.runtime.common;

/**
 * 能力实例生命周期状态。
 */
public enum CapabilityState {
    DISABLED,
    STARTING,
    WARMING,
    ACTIVE,
    DRAINING,
    MIGRATING,
    STOPPING,
    FAILED
}
