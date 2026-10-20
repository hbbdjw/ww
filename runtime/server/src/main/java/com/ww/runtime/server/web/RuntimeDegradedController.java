package com.ww.runtime.server.web;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.placement.DegradedPartition;
import com.ww.runtime.placement.DegradedPartitionStore;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.server.degraded.DegradedPartitionCache;

/**
 * P0-2 分区隔离管理端点（前缀 {@code /rt/degraded}，走 default-deny 鉴权）。
 *
 * <ul>
 *   <li>{@code GET /rt/degraded}：全部隔离分区列表（含属主当前是否存活）；</li>
 *   <li>{@code POST /rt/degraded/{capability}/{partition}/force-adopt?target=}：
 *       运营显式接受丢失的逃生通道——把隔离分区的放置 cutover 到指定存活节点
 *       （空数据），清除隔离标记。仅在属主确认永久丢失时使用；属主回归的
 *       正常恢复路径无需任何操作（调度器自动解除隔离）。</li>
 * </ul>
 */
@RestController
public class RuntimeDegradedController {

    private final DegradedPartitionStore degradedStore;
    private final PlacementStore placementStore;
    private final NodeRegistry nodeRegistry;
    private final DegradedPartitionCache degradedCache;

    public RuntimeDegradedController(DegradedPartitionStore degradedStore,
                                     PlacementStore placementStore,
                                     NodeRegistry nodeRegistry,
                                     DegradedPartitionCache degradedCache) {
        this.degradedStore = degradedStore;
        this.placementStore = placementStore;
        this.nodeRegistry = nodeRegistry;
        this.degradedCache = degradedCache;
    }

    @GetMapping("/rt/degraded")
    public Map<String, Object> list() {
        List<DegradedPartition> all = degradedStore.listAll();
        List<Map<String, Object>> items = all.stream().map(record -> {
            boolean ownerAlive = nodeRegistry.get(record.owner())
                    .map(info -> info.state() == NodeState.ACTIVE)
                    .orElse(false);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("capability", record.capability());
            item.put("partition", record.partition());
            item.put("owner", record.owner().value());
            item.put("ownerAlive", ownerAlive);
            item.put("sinceMs", record.sinceMs());
            item.put("reason", record.reason());
            return item;
        }).toList();
        return Map.of("count", items.size(), "items", items);
    }

    /**
     * 强制采用空分区（接受数据丢失）：cutover 放置到目标存活节点并清除隔离标记。
     *
     * <p>前置校验：分区必须处于隔离状态（404）；目标必须存在且 ACTIVE（400）；
     * cutover CAS 失败（并发变更）返回 409 可重试。</p>
     */
    @PostMapping("/rt/degraded/{capability}/{partition}/force-adopt")
    public ResponseEntity<Map<String, Object>> forceAdopt(@PathVariable String capability,
                                                          @PathVariable int partition,
                                                          @RequestParam String target) {
        if (!degradedStore.isDegraded(capability, partition)) {
            return ResponseEntity.status(404).body(Map.of(
                    "message", "partition " + capability + "/" + partition + " is not degraded"));
        }
        NodeId targetNode = NodeId.of(target);
        boolean targetAlive = nodeRegistry.get(targetNode)
                .map(info -> info.state() == NodeState.ACTIVE)
                .orElse(false);
        if (!targetAlive) {
            return ResponseEntity.badRequest().body(Map.of(
                    "message", "target node not ACTIVE: " + target));
        }
        Placement current = placementStore.get(capability, partition).orElse(null);
        if (current == null) {
            return ResponseEntity.status(404).body(Map.of(
                    "message", "no placement row for " + capability + "/" + partition));
        }
        if (!placementStore.cutover(capability, partition, targetNode, current.epoch())) {
            return ResponseEntity.status(409).body(Map.of(
                    "message", "cutover CAS failed (concurrent placement change); retry"));
        }
        degradedStore.clear(capability, partition);
        degradedCache.refreshNow();
        return ResponseEntity.ok(Map.of(
                "message", "partition " + capability + "/" + partition
                        + " force-adopted by " + target + " (empty data; loss accepted)",
                "previousOwner", current.node().value(),
                "newOwner", target));
    }
}
