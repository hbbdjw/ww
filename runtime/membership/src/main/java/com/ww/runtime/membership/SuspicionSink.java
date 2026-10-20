package com.ww.runtime.membership;

import com.ww.runtime.common.NodeId;

/**
 * 怀疑票回写：本地 Phi Accrual 检测器把 peer 判为疑似/死亡期间周期性投票，
 * 恢复健康后撤票；宿主侧适配集群多数仲裁存储（如注册表的怀疑票表），
 * 由 leader 计票达法定人数后唯一执行 DEAD 提升（PFAIL→FAIL）。
 *
 * <p>观察者身份（本节点）由宿主适配层填充，membership 不感知 NodeSuspicionStore。</p>
 */
public interface SuspicionSink {

    /** 投/刷新一张对 suspect 的怀疑票（每个检测周期持续疑似时重复调用以刷新 TTL）。 */
    void vote(NodeId suspect, double phi);

    /** 撤回对 suspect 的怀疑票（peer 恢复 ACTIVE / 本节点优雅退出）。 */
    void recall(NodeId suspect);

    /** 空实现：未开启多级仲裁时零开销（本地检测直接回写 DEAD 的传统语义）。 */
    SuspicionSink NOOP = new SuspicionSink() {
        @Override
        public void vote(NodeId suspect, double phi) {
        }

        @Override
        public void recall(NodeId suspect) {
        }
    };
}
