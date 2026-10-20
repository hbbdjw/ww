package com.ww.runtime.server.web;

import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.cluster.InMemoryNodeSuspicionStore;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;
import com.ww.runtime.server.RuntimeProperties;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET /rt/suspicions} 怀疑票可观测端点的单测：
 * 用内存注册表/票存储构造控制器，验证票数、法定人数与开关回显。
 */
class RuntimeAdminControllerSuspicionTest {

    private static final NodeId A = NodeId.of("A");
    private static final NodeId B = NodeId.of("B");
    private static final NodeId C = NodeId.of("C");

    private RuntimeProperties props(boolean quorumEnabled) {
        return new RuntimeProperties(
                new RuntimeProperties.Node("node-1", "localhost", 0, 1.0, ""),
                "memory", 1000, 3, 6, 3.0, 8.0, 2, 200, 3, 100L, 0L,
                new RuntimeProperties.Scheduler(10_000L, 3_000L, 2_000L),
                4096, 1, 0.10, 30_000, 30_000, 12_000L, 1, quorumEnabled, true,
                null, null, null,
                new RuntimeProperties.Security(30, 10), null, null);
    }

    private InMemoryNodeRegistry registryWithThreeActive() {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        for (NodeId id : List.of(A, B, C)) {
            registry.register(NodeInfo.of(id, "h", 9000, NodeState.JOINING, 0L, 1.0,
                    ResourceSnapshot.of(1.0, 100, 10, 100, 10, 0L)));
            registry.markState(id, NodeState.ACTIVE);
        }
        return registry;
    }

    private RuntimeAdminController controller(InMemoryNodeRegistry registry,
                                              InMemoryNodeSuspicionStore store,
                                              RuntimeProperties props) {
        return new RuntimeAdminController(null, null, null, registry, null, null, null,
                null, null, props, 8080, A, store, SystemClock.instance());
    }

    @Test
    @SuppressWarnings("unchecked")
    void suspicionsEndpointReportsVotesQuorumAndRequiredWhenEnabled() {
        InMemoryNodeRegistry registry = registryWithThreeActive();
        InMemoryNodeSuspicionStore store = new InMemoryNodeSuspicionStore();
        store.vote(B, A, 8.5);
        store.vote(B, C, 9.0);

        Map<String, Object> resp = controller(registry, store, props(true)).suspicions();

        assertThat(resp.get("enabled")).isEqualTo(true);
        assertThat(resp.get("voteTtlMs")).isEqualTo(12_000L);
        assertThat(resp.get("aliveNodes")).isEqualTo(3);
        assertThat(resp.get("required")).isEqualTo(2); // floor((3-1)/2)+1 = 2
        List<Map<String, Object>> votes = (List<Map<String, Object>>) resp.get("votes");
        assertThat(votes).singleElement().satisfies(row -> {
            assertThat(row.get("nodeId")).isEqualTo("B");
            assertThat(row.get("votes")).isEqualTo(2);
        });
    }

    @Test
    void suspicionsEndpointReportsDisabledAndEmptyWithoutQuorum() {
        InMemoryNodeRegistry registry = registryWithThreeActive();
        InMemoryNodeSuspicionStore store = new InMemoryNodeSuspicionStore();
        store.vote(B, A, 8.5);

        Map<String, Object> resp = controller(registry, store, props(false)).suspicions();
        assertThat(resp.get("enabled")).isEqualTo(false);
        // 开关关闭也照常暴露票数据供诊断，但不再有提升动作
        assertThat(resp.get("required")).isEqualTo(2);
    }
}
