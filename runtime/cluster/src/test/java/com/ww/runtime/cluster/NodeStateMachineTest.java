package com.ww.runtime.cluster;

import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeState;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeStateMachineTest {

    /** 与需求一致的静态合法跃迁表镜像（用于全矩阵校验）。 */
    private static final Map<NodeState, Set<NodeState>> LEGAL = Map.of(
            NodeState.JOINING, Set.of(NodeState.ACTIVE, NodeState.DEAD),
            NodeState.ACTIVE, Set.of(NodeState.SUSPECTED, NodeState.LEAVING, NodeState.DEAD),
            NodeState.SUSPECTED, Set.of(NodeState.ACTIVE, NodeState.DEAD),
            NodeState.LEAVING, Set.of(NodeState.DEAD),
            NodeState.DEAD, Set.of());

    @Test
    void legalTransitionsPass() {
        LEGAL.forEach((from, targets) -> targets.forEach(to ->
                assertThatCode(() -> NodeStateMachine.check(from, to))
                        .as("check(%s, %s) should be legal", from, to)
                        .doesNotThrowAnyException()));
    }

    @Test
    void illegalTransitionsThrow() {
        int illegal = 0;
        for (NodeState from : NodeState.values()) {
            for (NodeState to : NodeState.values()) {
                if (LEGAL.get(from).contains(to)) {
                    continue;
                }
                illegal++;
                assertThatThrownBy(() -> NodeStateMachine.check(from, to))
                        .as("check(%s, %s) should be illegal", from, to)
                        .isInstanceOf(IllegalStateTransitionException.class)
                        .hasMessageContaining(from + " -> " + to);
            }
        }
        // 25 种组合 - 8 种合法（2+3+2+1+0）= 17 种非法，确保矩阵被完整遍历
        assertThat(illegal).isEqualTo(17);
    }

    @Test
    void deadIsTerminal() {
        for (NodeState to : NodeState.values()) {
            assertThatThrownBy(() -> NodeStateMachine.check(NodeState.DEAD, to))
                    .isInstanceOf(IllegalStateTransitionException.class);
        }
    }

    @Test
    void sameStateTransitionsAreIllegal() {
        for (NodeState state : NodeState.values()) {
            assertThatThrownBy(() -> NodeStateMachine.check(state, state))
                    .isInstanceOf(IllegalStateTransitionException.class);
        }
    }

    @Test
    void nullArgumentsRejected() {
        assertThatThrownBy(() -> NodeStateMachine.check(null, NodeState.ACTIVE))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> NodeStateMachine.check(NodeState.ACTIVE, null))
                .isInstanceOf(NullPointerException.class);
    }
}
