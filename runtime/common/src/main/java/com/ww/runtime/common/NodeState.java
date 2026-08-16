package com.ww.runtime.common;

/**
 * 节点生命周期状态。
 */
public enum NodeState {
    JOINING,
    ACTIVE,
    SUSPECTED,
    DEAD,
    LEAVING
}
