package com.ww.runtime.membership;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeState;

/**
 * 状态回写：故障检测得出新状态后回写宿主侧注册表。
 * <p>宿主侧通常适配 {@code NodeRegistry.markState}。</p>
 */
@FunctionalInterface
public interface StateSink {

    /** 将节点 {@code id} 的状态回写为 {@code newState}。 */
    void apply(NodeId id, NodeState newState);
}
