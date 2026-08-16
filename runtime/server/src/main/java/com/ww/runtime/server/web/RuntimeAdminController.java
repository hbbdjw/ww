package com.ww.runtime.server.web;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.membership.MembershipService;
import com.ww.runtime.migration.MigrationTaskStore;
import com.ww.runtime.placement.Partitioner;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.scheduler.DesiredStateWriter;
import com.ww.runtime.scheduler.ReconcileLoop;
import com.ww.runtime.server.RuntimeProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * REST 管理端点（前缀 {@code /rt}）。
 *
 * <p>§60 约束：管理面<b>只写期望态</b>（desired）。不提供任何直接修改 actual 的端点——
 * actual 由各节点能力状态机自治产出，偏差由调度调谐循环收敛。</p>
 */
@RestController
@RequestMapping("/rt")
public class RuntimeAdminController {

    private final ReconcileLoop reconcileLoop;
    private final DesiredStateWriter desiredStateWriter;
    private final CapabilityStateStore capabilityStateStore;
    private final NodeRegistry nodeRegistry;
    private final MembershipService membershipService;
    private final MigrationTaskStore migrationTaskStore;
    private final PlacementStore placementStore;
    private final RuntimeProperties props;
    private final NodeId self;
    /** 本节点 HTTP 端口（仅供参考：NodeInfo 未注册属主节点的 HTTP 端口，
     *  客户端需按部署约定由 ownerNode 推导其数据面端口）。 */
    private final int httpPort;

    public RuntimeAdminController(ReconcileLoop reconcileLoop,
                                  DesiredStateWriter desiredStateWriter,
                                  CapabilityStateStore capabilityStateStore,
                                  NodeRegistry nodeRegistry,
                                  MembershipService membershipService,
                                  MigrationTaskStore migrationTaskStore,
                                  PlacementStore placementStore,
                                  RuntimeProperties props,
                                  @Value("${server.port:8080}") int httpPort,
                                  NodeId self) {
        this.reconcileLoop = reconcileLoop;
        this.desiredStateWriter = desiredStateWriter;
        this.capabilityStateStore = capabilityStateStore;
        this.nodeRegistry = nodeRegistry;
        this.membershipService = membershipService;
        this.migrationTaskStore = migrationTaskStore;
        this.placementStore = placementStore;
        this.props = props;
        this.httpPort = httpPort;
        this.self = self;
    }

    /**
     * 集群状态快照：调度器视图（leader/租约/能力记录/放置主表）+ 本节点注册视图
     * + 成员关系视图 + 活动迁移任务数。响应均为 record/POJO，Jackson 直接序列化。
     */
    @GetMapping("/status")
    public Map<String, Object> status() {
        ReconcileLoop.ClusterStatus snapshot = reconcileLoop.snapshot();
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("nodeId", self.value());
        status.put("leader", snapshot.leader());
        status.put("leaseOwner", snapshot.leaseOwner());
        status.put("capabilities", snapshot.capabilities());
        status.put("primariesByCapability", snapshot.primariesByCapability());
        status.put("self", nodeRegistry.get(self).orElse(null));
        status.put("members", membershipService.view());
        status.put("activeMigrationTasks", migrationTaskStore.listActive().size());
        return status;
    }

    /**
     * 写入期望态（仅 ACTIVE / DISABLED 两个开关值），body 如 {@code {"desired":"ACTIVE"}}。
     *
     * <p>非法输入（未知能力类型 / 非法状态值）与业务拒绝（清空最后节点保护）均返回
     * 400 + message；成功返回受影响的 CapabilityRecord（200）。</p>
     */
    @PutMapping("/nodes/{nodeId}/capabilities/{type}/desired")
    public ResponseEntity<Object> setDesired(@PathVariable("nodeId") String nodeId,
                                             @PathVariable("type") String type,
                                             @RequestBody DesiredRequest body) {
        NodeId node;
        try {
            node = NodeId.of(nodeId);
        } catch (RuntimeException e) {
            return badRequest(e.getMessage());
        }
        CapabilityType capability;
        try {
            capability = CapabilityType.parse(type);
        } catch (RuntimeException e) {
            return badRequest(e.getMessage());
        }
        String desired = body == null ? null : body.desired();
        CapabilityState state;
        try {
            state = CapabilityState.valueOf(String.valueOf(desired).trim().toUpperCase(Locale.ROOT));
        } catch (RuntimeException e) {
            return badRequest("desired must be ACTIVE or DISABLED, got: " + desired);
        }
        try {
            desiredStateWriter.setDesired(node, capability, state);
        } catch (IllegalArgumentException | IllegalStateTransitionException e) {
            return badRequest(e.getMessage());
        }
        return ResponseEntity.ok(capabilityStateStore.get(node, capability).orElse(null));
    }

    /** 全量节点视图（含 DEAD 墓碑行）。 */
    @GetMapping("/nodes")
    public List<com.ww.runtime.common.NodeInfo> nodes() {
        return nodeRegistry.listAll();
    }

    /**
     * 分区路由查询：返回指定能力 key 的分区号与属主节点地址，
     * 客户端据此直连属主节点的数据面端点（写操作在属主节点上带 epoch fencing）。
     */
    @GetMapping("/placement/{capability}/lookup")
    public ResponseEntity<Object> lookup(@PathVariable("capability") String capability,
                                         @RequestParam("key") String key) {
        CapabilityType type;
        try {
            type = CapabilityType.parse(capability);
        } catch (RuntimeException e) {
            return badRequest(e.getMessage());
        }
        if (key == null || key.isBlank()) {
            return badRequest("key must not be blank");
        }
        int partition = Partitioner.partition(key, props.partitionCount());
        Optional<Placement> placement = placementStore.get(type.name(), partition);
        if (placement.isEmpty()) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("message", "partition has no owner yet");
            body.put("capability", type.name());
            body.put("partition", partition);
            return ResponseEntity.status(404).body(body);
        }
        Placement p = placement.get();
        com.ww.runtime.common.NodeInfo ownerInfo = nodeRegistry.get(p.node()).orElse(null);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("capability", type.name());
        body.put("key", key);
        body.put("partition", partition);
        body.put("ownerNode", p.node().value());
        body.put("epoch", p.epoch());
        body.put("host", ownerInfo == null ? null : ownerInfo.host());
        body.put("grpcPort", ownerInfo == null ? -1 : ownerInfo.grpcPort());
        // 注：注册表未存属主节点 HTTP 端口，此字段为「被查询节点」的端口，
        // 客户端应按部署约定由 ownerNode 推导其数据面端口（如 compose 中 node-N → 809N）
        body.put("queriedNodeHttpPort", httpPort);
        return ResponseEntity.ok(body);
    }

    /** 期望态写入请求体。 */
    public record DesiredRequest(String desired) {
    }

    private static ResponseEntity<Object> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("message", String.valueOf(message)));
    }
}
