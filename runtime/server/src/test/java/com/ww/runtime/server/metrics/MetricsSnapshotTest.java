package com.ww.runtime.server.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;

import tools.jackson.databind.json.JsonMapper;

/**
 * {@link MetricsStreamController#buildSnapshot(long)} 的纯组装测试：
 * 不启动 Web 容器与推送调度，peer 端口为 0 时降级路径不发起任何 HTTP 请求。
 */
class MetricsSnapshotTest {

    private static final NodeId SELF = NodeId.of("node-self");
    private static final NodeId PEER_A = NodeId.of("node-a");
    private static final NodeId PEER_DEAD = NodeId.of("node-dead");

    private MetricsStreamController newController(InMemoryNodeRegistry registry,
                                                   RequestMetricsCollector collector) {
        return new MetricsStreamController(collector, registry, SELF, JsonMapper.shared());
    }

    private static NodeInfo node(NodeId id, int httpPort, NodeState state) {
        ResourceSnapshot resources = new ResourceSnapshot(0, 0, 0, 0, 0, 0L);
        return new NodeInfo(id, "127.0.0.1", 9000, httpPort, state, 1L, 1.0d, resources);
    }

    @Test
    @SuppressWarnings("unchecked")
    void localNodeAlwaysPresentWithSecondBucketCount() {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        RequestMetricsCollector collector = new RequestMetricsCollector(60);
        MetricsStreamController controller = newController(registry, collector);

        long second = 500L;
        collector.recordAt(second);
        collector.recordAt(second);

        Map<String, Object> snapshot = controller.buildSnapshot(second);

        assertThat(snapshot.get("timestamp")).isEqualTo((second + 1) * 1000L);
        assertThat(snapshot.get("second")).isEqualTo(second);
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) snapshot.get("nodes");
        assertThat(nodes).hasSize(1);
        assertThat(nodes.get(0).get("nodeId")).isEqualTo("node-self");
        // 注册表尚无自注册行时以 JOINING 兜底
        assertThat(nodes.get(0).get("state")).isEqualTo("JOINING");
        assertThat(nodes.get(0).get("rps")).isEqualTo(2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void peerWithoutHttpPortDegradesToNullAndDeadPeerExcluded() {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        // self 注册为 ACTIVE
        registry.register(node(SELF, 8080, NodeState.JOINING));
        registry.markState(SELF, NodeState.ACTIVE);
        // peer 存活但未上报 HTTP 端口 → 无法拉取，rps=null（不发起网络请求）
        registry.register(node(PEER_A, 0, NodeState.ACTIVE));
        // DEAD 节点不在 alive 集合，折线不再推送（tombstone 直接置 DEAD，不走状态机）
        registry.register(node(PEER_DEAD, 8082, NodeState.JOINING));
        registry.tombstone(PEER_DEAD);

        RequestMetricsCollector collector = new RequestMetricsCollector(60);
        MetricsStreamController controller = newController(registry, collector);

        long second = 700L;
        collector.recordAt(second);
        Map<String, Object> snapshot = controller.buildSnapshot(second);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) snapshot.get("nodes");
        assertThat(nodes).hasSize(2);
        // nodeId 字典序：node-a 在 node-self 前
        assertThat(nodes.get(0).get("nodeId")).isEqualTo("node-a");
        assertThat(nodes.get(0).get("rps")).isNull();
        assertThat(nodes.get(1).get("nodeId")).isEqualTo("node-self");
        assertThat(nodes.get(1).get("state")).isEqualTo("ACTIVE");
        assertThat(nodes.get(1).get("rps")).isEqualTo(1L);
    }
}
