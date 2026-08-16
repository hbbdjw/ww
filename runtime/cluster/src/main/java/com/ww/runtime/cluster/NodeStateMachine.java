package com.ww.runtime.cluster;

import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeState;

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
 * DEAD      -> {}（终态）
 * </pre>
 *
 * <p>同态跃迁（如 ACTIVE -&gt; ACTIVE）不在合法集合内，视为非法。</p>
 */
public final class NodeStateMachine {

    private static final Map<NodeState, Set<NodeState>> TRANSITIONS = Map.of(
            NodeState.JOINING, EnumSet.of(NodeState.ACTIVE, NodeState.DEAD),
            NodeState.ACTIVE, EnumSet.of(NodeState.SUSPECTED, NodeState.LEAVING, NodeState.DEAD),
            NodeState.SUSPECTED, EnumSet.of(NodeState.ACTIVE, NodeState.DEAD),
            NodeState.LEAVING, EnumSet.of(NodeState.DEAD),
            NodeState.DEAD, EnumSet.noneOf(NodeState.class));

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
}
