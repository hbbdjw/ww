package com.ww.runtime.cluster;

import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeState;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 节点生命周期状态机：静态合法跃迁表。
 *
 * <pre>
 * JOINING   -> {ACTIVE, DEAD}
 * ACTIVE    -> {SUSPECTED, LEAVING, DEAD}
 * SUSPECTED -> {ACTIVE, DEAD}
 * LEAVING   -> {DEAD}
 * DEAD      -> {JOINING}（仅经重新注册重入集群）
 * </pre>
 *
 * <p>同态跃迁（如 ACTIVE -&gt; ACTIVE）不在合法集合内，视为非法。</p>
 *
 * <p>DEAD 节点复活（重启/重连）必须走完整的重新注册流程
 * （register → JOINING → 自检 → ACTIVE）。DEAD → ACTIVE 为非法跃迁：
 * 仅凭收到消息不足以证明节点完整回归（可能是残留重传），且注册流程
 * 会重置 epoch/资源快照等元数据，跳过会导致视图与注册表不一致。</p>
 */
public final class NodeStateMachine {

    private static final Map<NodeState, Set<NodeState>> TRANSITIONS = Map.of(
            NodeState.JOINING, EnumSet.of(NodeState.ACTIVE, NodeState.DEAD),
            NodeState.ACTIVE, EnumSet.of(NodeState.SUSPECTED, NodeState.LEAVING, NodeState.DEAD),
            NodeState.SUSPECTED, EnumSet.of(NodeState.ACTIVE, NodeState.DEAD),
            NodeState.LEAVING, EnumSet.of(NodeState.DEAD),
            NodeState.DEAD, EnumSet.of(NodeState.JOINING));

    /** 目标态 → 合法前驱集合（TRANSITIONS 的逆向索引，S9 单条 UPDATE 的 WHERE state IN 依据）。 */
    private static final Map<NodeState, Set<NodeState>> PREDECESSORS;

    static {
        Map<NodeState, Set<NodeState>> pred = new EnumMap<>(NodeState.class);
        for (NodeState to : NodeState.values()) {
            pred.put(to, EnumSet.noneOf(NodeState.class));
        }
        TRANSITIONS.forEach((from, tos) -> tos.forEach(to -> pred.get(to).add(from)));
        PREDECESSORS = Collections.unmodifiableMap(pred);
    }

    private NodeStateMachine() {
    }

    /**
     * 校验 from -> to 是否为合法跃迁，非法时抛出 {@link IllegalStateTransitionException}。
     */
    public static void check(NodeState from, NodeState to) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        if (!TRANSITIONS.get(from).contains(to)) {
            throw new IllegalStateTransitionException(from, to);
        }
    }

    /** 目标态的合法前驱集合（不含目标态自身——同态跃迁按状态机定义为非法）。 */
    public static Set<NodeState> legalPredecessors(NodeState to) {
        return PREDECESSORS.getOrDefault(Objects.requireNonNull(to, "to"), Set.of());
    }
}
