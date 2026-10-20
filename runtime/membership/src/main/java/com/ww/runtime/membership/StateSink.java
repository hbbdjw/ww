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

    /**
     * S12 带防护回写：宿主实现应比较注册表当前 {@code state_epoch} 与判定时刻的
     * {@code observedEpoch}——注册表更高说明判定基于陈旧信息（期间节点已重新注册或被
     * 其他实例先行回写），须丢弃本条陈旧决定。
     *
     * @param observedEpoch  做出该判定时本地视图中节点的 state_epoch；&lt;0 表示无纪元信息
     * @param directEvidence true = 信息来自节点自身直报（自报 ACTIVE 等）：节点是自身状态的
     *                       绝对权威，必须豁免纪元防护（重注册后 epoch 被重置，
     *                       数字比较会把合法复活一并拦死）
     */
    default void apply(NodeId id, NodeState newState, long observedEpoch, boolean directEvidence) {
        apply(id, newState);
    }
}
