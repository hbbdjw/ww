package com.ww.runtime.server.web;

import com.ww.runtime.cluster.InMemoryNodeRegistry;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.placement.InMemoryDegradedPartitionStore;
import com.ww.runtime.placement.InMemoryPlacementStore;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.server.degraded.DegradedPartitionCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RuntimeDegradedController：隔离列表 + force-adopt 逃生通道（显式接受丢失）。
 */
class RuntimeDegradedControllerTest {

    private static final NodeId DEAD = NodeId.of("node-0");
    private static final NodeId LIVE = NodeId.of("node-2");

    private InMemoryDegradedPartitionStore degraded;
    private InMemoryPlacementStore placements;
    private InMemoryNodeRegistry registry;
    private DegradedPartitionCache cache;
    private RuntimeDegradedController controller;

    @BeforeEach
    void setUp() {
        degraded = new InMemoryDegradedPartitionStore();
        placements = new InMemoryPlacementStore();
        registry = new InMemoryNodeRegistry();
        registry.register(NodeInfo.of(DEAD, "h0", 9000, NodeState.ACTIVE, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, 0L)));
        registry.register(NodeInfo.of(LIVE, "h2", 9000, NodeState.ACTIVE, 0L, 1.0d,
                ResourceSnapshot.of(1.0d, 100, 10, 100, 10, 0L)));
        // register 恒落 JOINING：显式推进到目标态
        registry.markState(DEAD, NodeState.DEAD);
        registry.markState(LIVE, NodeState.ACTIVE);
        cache = new DegradedPartitionCache(degraded);
        controller = new RuntimeDegradedController(degraded, placements, registry, cache);
    }

    @Test
    @SuppressWarnings("unchecked")
    void listReportsDegradedPartitionsWithOwnerLiveness() {
        degraded.mark("CACHE", 7, DEAD, "no live replica");
        degraded.mark("CACHE", 9, LIVE, "no live replica");

        Map<String, Object> body = controller.list();

        assertThat(body.get("count")).isEqualTo(2);
        var items = (Iterable<Map<String, Object>>) body.get("items");
        var byPartition = new java.util.HashMap<Integer, Map<String, Object>>();
        items.forEach(item -> byPartition.put((Integer) item.get("partition"), item));
        assertThat(byPartition.get(7).get("ownerAlive")).isEqualTo(false);
        assertThat(byPartition.get(9).get("ownerAlive")).isEqualTo(true);
        assertThat(byPartition.get(7).get("owner")).isEqualTo("node-0");
    }

    @Test
    void forceAdoptRejectsNonDegradedPartition() {
        ResponseEntity<Map<String, Object>> response =
                controller.forceAdopt("CACHE", 7, LIVE.value());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void forceAdoptRejectsNonActiveTarget() {
        degraded.mark("CACHE", 7, DEAD, "no live replica");

        ResponseEntity<Map<String, Object>> response =
                controller.forceAdopt("CACHE", 7, DEAD.value()); // 目标是死节点

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(degraded.isDegraded("CACHE", 7)).isTrue(); // 隔离未解除
    }

    @Test
    void forceAdoptCutoversPlacementAndClearsQuarantine() {
        degraded.mark("CACHE", 7, DEAD, "no live replica");
        placements.replaceAll("CACHE", java.util.List.of(
                new Placement(7, DEAD, PlacementRole.PRIMARY, 5L)));

        ResponseEntity<Map<String, Object>> response =
                controller.forceAdopt("CACHE", 7, LIVE.value());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsEntry("previousOwner", "node-0");
        assertThat(response.getBody()).containsEntry("newOwner", LIVE.value());
        // 放置切到目标存活节点（epoch+1），隔离标记清除
        assertThat(placements.get("CACHE", 7).orElseThrow().node()).isEqualTo(LIVE);
        assertThat(placements.get("CACHE", 7).orElseThrow().epoch()).isEqualTo(6L);
        assertThat(degraded.isDegraded("CACHE", 7)).isFalse();
        // 数据面视图立即刷新（不等 1s TTL）
        cache.start();
        assertThat(cache.isDegraded("CACHE", 7)).isFalse();
        cache.stop();
    }
}
