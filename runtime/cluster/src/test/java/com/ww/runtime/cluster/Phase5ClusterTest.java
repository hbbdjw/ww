package com.ww.runtime.cluster;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.grpc.GrpcServerHost;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 阶段 5 验收（S2 / §3.12①）：被误判 DEAD 的健康节点经 {@code reRegister()} 复活——
 * register 重置纪元（合法路径 DEAD→JOINING→ACTIVE），伸缩视图重新纳入。
 */
@DisplayName("阶段 5 验收：S2 自动重注册复活")
class Phase5ClusterTest {

    private static final NodeId SELF = NodeId.of("node-x");

    @Test
    @DisplayName("reRegister：DEAD → epoch 重置 → ACTIVE，listAlive 重新包含")
    void reRegisterRevivesDeadRowWithEpochReset() {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        registry.register(NodeInfo.of(SELF, "localhost", 9100, NodeState.JOINING, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, 0L)));
        registry.markState(SELF, NodeState.ACTIVE);
        registry.markState(SELF, NodeState.SUSPECTED);
        registry.markState(SELF, NodeState.DEAD); // 长 GC 停顿误判
        long deadEpoch = registry.get(SELF).orElseThrow().stateEpoch();
        assertThat(registry.listAlive()).isEmpty();

        NodeRuntime runtime = new NodeRuntime(SELF, "localhost", 9100, 8080, 1.0d,
                registry, new GrpcServerHost(), () -> ResourceSnapshot.of(1.0d, 100, 10, 100, 10, 5L),
                SystemClock.instance(), 1_000L);
        runtime.reRegister();

        NodeInfo revived = registry.get(SELF).orElseThrow();
        assertThat(revived.state()).isEqualTo(NodeState.ACTIVE);
        assertThat(revived.stateEpoch())
                .as("复活必须经 register 纪元重置（红线：不走 DEAD→ACTIVE 捷径）")
                .isLessThan(deadEpoch);
        assertThat(registry.listAlive()).extracting(info -> info.id()).containsExactly(SELF);
    }
}
