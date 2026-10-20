package com.ww.runtime.cluster;

import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryNodeRegistryTest {

    private final InMemoryNodeRegistry registry = new InMemoryNodeRegistry();

    private static ResourceSnapshot snapshot(long used) {
        return ResourceSnapshot.of(10.0, 1000L, used, 2000L, used, 42L);
    }

    private static NodeInfo node(String id, NodeState state, long epoch, long used) {
        return NodeInfo.of(NodeId.of(id), "host-" + id, 9000, state, epoch, 1.0, snapshot(used));
    }

    private NodeInfo registered(String id) {
        registry.register(node(id, NodeState.JOINING, 0L, 10L));
        return registry.get(NodeId.of(id)).orElseThrow();
    }

    // ---- register ----

    @Test
    void registerInsertsAsJoiningWithResetEpoch() {
        registry.register(node("n1", NodeState.ACTIVE, 9L, 10L));

        NodeInfo info = registry.get(NodeId.of("n1")).orElseThrow();
        assertThat(info.state()).isEqualTo(NodeState.JOINING);
        assertThat(info.stateEpoch()).isZero();
        assertThat(info.host()).isEqualTo("host-n1");
        assertThat(info.grpcPort()).isEqualTo(9000);
        assertThat(info.weight()).isEqualTo(1.0);
        assertThat(info.resources()).isEqualTo(snapshot(10L));
    }

    @Test
    void registerUpsertsExistingNodeWithFreshIdentity() {
        registry.register(node("n1", NodeState.JOINING, 0L, 10L));
        registry.markState(NodeId.of("n1"), NodeState.ACTIVE);

        // upsert：host/端口/权重/快照取新值，状态重置 JOINING、epoch 清零
        registry.register(NodeInfo.of(NodeId.of("n1"), "new-host", 7000,
                NodeState.DEAD, 77L, 2.5, snapshot(99L)));

        NodeInfo info = registry.get(NodeId.of("n1")).orElseThrow();
        assertThat(info.host()).isEqualTo("new-host");
        assertThat(info.grpcPort()).isEqualTo(7000);
        assertThat(info.weight()).isEqualTo(2.5);
        assertThat(info.resources()).isEqualTo(snapshot(99L));
        assertThat(info.state()).isEqualTo(NodeState.JOINING);
        assertThat(info.stateEpoch()).isZero();
    }

    // ---- heartbeat ----

    @Test
    void heartbeatRefreshesResourcesAndKeepsState() {
        registry.register(node("n1", NodeState.JOINING, 0L, 10L));
        registry.markState(NodeId.of("n1"), NodeState.ACTIVE);

        registry.heartbeat(NodeId.of("n1"), snapshot(500L));

        NodeInfo info = registry.get(NodeId.of("n1")).orElseThrow();
        assertThat(info.resources()).isEqualTo(snapshot(500L));
        assertThat(info.state()).isEqualTo(NodeState.ACTIVE);
        assertThat(info.stateEpoch()).isEqualTo(1L);
    }

    @Test
    void heartbeatUnknownNodeThrows() {
        assertThatThrownBy(() -> registry.heartbeat(NodeId.of("ghost"), snapshot(1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ghost");
        assertThat(registry.listAll()).isEmpty();
    }

    @Test
    void heartbeatRejectsNullArguments() {
        NodeId id = NodeId.of("n1");
        registry.register(node("n1", NodeState.JOINING, 0L, 10L));
        assertThatThrownBy(() -> registry.heartbeat(null, snapshot(1L)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> registry.heartbeat(id, null))
                .isInstanceOf(NullPointerException.class);
    }

    // ---- markState ----

    @Test
    void markStateFollowsStateMachineAndIncrementsEpoch() {
        registry.register(node("n1", NodeState.JOINING, 0L, 10L));
        NodeId n1 = NodeId.of("n1");

        registry.markState(n1, NodeState.ACTIVE);          // epoch 1
        registry.markState(n1, NodeState.SUSPECTED);       // epoch 2
        registry.markState(n1, NodeState.ACTIVE);          // epoch 3

        NodeInfo info = registry.get(n1).orElseThrow();
        assertThat(info.state()).isEqualTo(NodeState.ACTIVE);
        assertThat(info.stateEpoch()).isEqualTo(3L);
    }

    @Test
    void markStateRejectsIllegalTransitions() {
        NodeId n1 = NodeId.of("n1");
        NodeId n2 = NodeId.of("n2");
        NodeId n3 = NodeId.of("n3");
        NodeId n4 = NodeId.of("n4");
        registry.register(node("n1", NodeState.JOINING, 0L, 10L));
        registry.register(node("n2", NodeState.JOINING, 0L, 10L));
        registry.register(node("n3", NodeState.JOINING, 0L, 10L));
        registry.register(node("n4", NodeState.JOINING, 0L, 10L));
        registry.markState(n2, NodeState.ACTIVE);
        registry.markState(n3, NodeState.ACTIVE);
        registry.markState(n4, NodeState.ACTIVE);
        registry.markState(n4, NodeState.DEAD);

        // JOINING -> LEAVING 非法
        assertThatThrownBy(() -> registry.markState(n1, NodeState.LEAVING))
                .isInstanceOf(IllegalStateTransitionException.class)
                .hasMessageContaining("JOINING -> LEAVING");
        // ACTIVE -> JOINING 非法
        assertThatThrownBy(() -> registry.markState(n2, NodeState.JOINING))
                .isInstanceOf(IllegalStateTransitionException.class)
                .hasMessageContaining("ACTIVE -> JOINING");
        // 同态 ACTIVE -> ACTIVE 非法
        assertThatThrownBy(() -> registry.markState(n3, NodeState.ACTIVE))
                .isInstanceOf(IllegalStateTransitionException.class);
        // DEAD 复活仅允许经重新注册（DEAD -> ACTIVE 捷径非法）
        assertThatThrownBy(() -> registry.markState(n4, NodeState.ACTIVE))
                .isInstanceOf(IllegalStateTransitionException.class)
                .hasMessageContaining("DEAD -> ACTIVE");

        // 非法跃迁不得改变 epoch
        assertThat(registry.get(n2).orElseThrow().stateEpoch()).isEqualTo(1L);
        assertThat(registry.get(n4).orElseThrow().state()).isEqualTo(NodeState.DEAD);
    }

    @Test
    void markStateUnknownNodeThrows() {
        assertThatThrownBy(() -> registry.markState(NodeId.of("ghost"), NodeState.ACTIVE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ghost");
    }

    // ---- listAlive / listAll ----

    @Test
    void listAliveFiltersDeadAndLeaving() {
        registered("joining");
        registered("active");
        registered("suspected");
        registered("leaving");
        registered("dead");
        registry.markState(NodeId.of("active"), NodeState.ACTIVE);
        registry.markState(NodeId.of("suspected"), NodeState.ACTIVE);
        registry.markState(NodeId.of("suspected"), NodeState.SUSPECTED);
        registry.markState(NodeId.of("leaving"), NodeState.ACTIVE);
        registry.markState(NodeId.of("leaving"), NodeState.LEAVING);
        registry.markState(NodeId.of("dead"), NodeState.DEAD);

        List<NodeInfo> alive = registry.listAlive();
        assertThat(alive).extracting(NodeInfo::id)
                .containsExactlyInAnyOrder(NodeId.of("joining"), NodeId.of("active"), NodeId.of("suspected"));

        assertThat(registry.listAll()).hasSize(5);
    }

    @Test
    void getAbsentReturnsEmpty() {
        assertThat(registry.get(NodeId.of("nobody"))).isEqualTo(Optional.empty());
    }

    // ---- tombstone ----

    @Test
    void tombstoneForcesDeadKeepsRowAndIsIdempotent() {
        registered("n1");
        registry.markState(NodeId.of("n1"), NodeState.ACTIVE);

        registry.tombstone(NodeId.of("n1"));
        NodeInfo dead = registry.get(NodeId.of("n1")).orElseThrow();
        assertThat(dead.state()).isEqualTo(NodeState.DEAD);
        assertThat(dead.stateEpoch()).isEqualTo(2L); // JOINING(0) -> ACTIVE(1) -> DEAD(2)
        assertThat(registry.listAlive()).isEmpty();
        assertThat(registry.listAll()).hasSize(1); // 行保留

        // 已 DEAD：幂等，epoch 不再前进
        registry.tombstone(NodeId.of("n1"));
        assertThat(registry.get(NodeId.of("n1")).orElseThrow().stateEpoch()).isEqualTo(2L);

        // 未知节点：不抛异常、不产生行
        registry.tombstone(NodeId.of("ghost"));
        assertThat(registry.get(NodeId.of("ghost"))).isEmpty();
        assertThat(registry.listAll()).hasSize(1);
    }

    // ---- 重启恢复语义 ----

    @Test
    void reRegisterAfterDeathAllowsStateToAdvanceAgain() {
        NodeId n1 = NodeId.of("n1");
        registered("n1");
        registry.markState(n1, NodeState.ACTIVE);
        registry.markState(n1, NodeState.SUSPECTED);
        registry.markState(n1, NodeState.DEAD);
        assertThat(registry.listAlive()).isEmpty();

        // 同一 NodeId 重启后再 register：回到 JOINING/epoch 0，状态机可重新推进
        registry.register(node("n1", NodeState.JOINING, 0L, 10L));
        NodeInfo rejoined = registry.get(n1).orElseThrow();
        assertThat(rejoined.state()).isEqualTo(NodeState.JOINING);
        assertThat(rejoined.stateEpoch()).isZero();

        registry.markState(n1, NodeState.ACTIVE);
        assertThat(registry.get(n1).orElseThrow().state()).isEqualTo(NodeState.ACTIVE);
        assertThat(registry.get(n1).orElseThrow().stateEpoch()).isEqualTo(1L);
        assertThat(registry.listAlive()).singleElement().satisfies(info -> {
            assertThat(info.state()).isEqualTo(NodeState.ACTIVE);
            assertThat(info.stateEpoch()).isEqualTo(1L);
        });
    }
}
