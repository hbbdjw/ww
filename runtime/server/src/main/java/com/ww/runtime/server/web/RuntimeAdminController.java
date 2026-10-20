package com.ww.runtime.server.web;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.cluster.NodeRuntime;
import com.ww.runtime.cluster.decommission.DecommissionStore;
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
    private final DecommissionStore decommissionStore;
    private final NodeRuntime nodeRuntime;
    private final RuntimeProperties props;
    private final NodeId self;
    /** 本节点 HTTP 端口（仅供参考：NodeInfo 未注册属主节点的 HTTP 端口，
     *  客户端需按部署约定由 ownerNode 推导其数据面端口）。 */
    private final int httpPort;
    private final com.ww.runtime.cluster.NodeSuspicionStore suspicionStore;
    private final com.ww.runtime.common.Clock clock;

    public RuntimeAdminController(ReconcileLoop reconcileLoop,
                                  DesiredStateWriter desiredStateWriter,
                                  CapabilityStateStore capabilityStateStore,
                                  NodeRegistry nodeRegistry,
                                  MembershipService membershipService,
                                  MigrationTaskStore migrationTaskStore,
                                  PlacementStore placementStore,
                                  DecommissionStore decommissionStore,
                                  NodeRuntime nodeRuntime,
                                  RuntimeProperties props,
                                  @Value("${server.port:8080}") int httpPort,
                                  NodeId self,
                                  com.ww.runtime.cluster.NodeSuspicionStore suspicionStore,
                                  com.ww.runtime.common.Clock clock) {
        this.reconcileLoop = reconcileLoop;
        this.desiredStateWriter = desiredStateWriter;
        this.capabilityStateStore = capabilityStateStore;
        this.nodeRegistry = nodeRegistry;
        this.membershipService = membershipService;
        this.migrationTaskStore = migrationTaskStore;
        this.placementStore = placementStore;
        this.decommissionStore = decommissionStore;
        this.nodeRuntime = nodeRuntime;
        this.props = props;
        this.httpPort = httpPort;
        this.self = self;
        this.suspicionStore = suspicionStore;
        this.clock = clock;
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
        // 前端期望 leader 为主节点 id（string|null）；isLeader 保留本节点视角布尔值供诊断
        status.put("leader", snapshot.leaseOwner());
        status.put("isLeader", snapshot.leader());
        status.put("leaseOwner", snapshot.leaseOwner());
        status.put("capabilities", snapshot.capabilities().stream().map(RuntimeAdminController::capabilityDto).toList());
        status.put("primariesByCapability", snapshot.primariesByCapability());
        status.put("self", nodeRegistry.get(self).map(RuntimeAdminController::nodeDto).orElse(null));
        // members 需输出数组（前端 ClusterStatus.members: NodeInfo[] 依赖 sort/map），
        // 且 nodeId/host/grpcPort/state 为扁平字段，不能直接暴露内部 record（NodeId 会序列化成对象）；
        // phi 为本节点 φ 检测器对该 peer 的当前怀疑度（本地视角观测值，非集群聚合）。
        status.put("members", membershipService.view().values().stream()
                .map(this::memberDto).toList());
        status.put("activeMigrationTasks", migrationTaskStore.listActive().size());
        status.put("decommissions", decommissionStore.listActive().stream()
                .map(r -> Map.of("nodeId", r.node().value(), "state", r.state().name())).toList());
        return status;
    }

    /**
     * 怀疑票视图（多级故障确认可观测）：开关/TTL/存活节点数/法定人数 + 每个 suspect
     * 的 TTL 内新鲜票数。开关关闭时数据仍返回供诊断（无提升动作）。
     */
    @GetMapping("/suspicions")
    public Map<String, Object> suspicions() {
        long ttl = props.suspicionVoteTtlMs();
        long now = clock.nowMillis();
        int aliveCount = nodeRegistry.listAlive().size();
        int required = Math.min(
                Math.max(props.suspicionMinObservers(), (aliveCount - 1) / 2 + 1),
                Math.max(1, aliveCount - 1));
        Map<NodeId, Integer> counts = suspicionStore.freshVoteCounts(now, ttl);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("enabled", props.suspicionQuorumEnabled());
        resp.put("voteTtlMs", ttl);
        resp.put("aliveNodes", aliveCount);
        resp.put("required", required);
        resp.put("votes", counts.entrySet().stream()
                .map(e -> Map.of("nodeId", e.getKey().value(), "votes", e.getValue()))
                .toList());
        return resp;
    }

    /** 成员 DTO：nodeDto 基础字段 + 本节点 φ 检测器对该 peer 的当前怀疑度（保留两位小数）。 */
    private Map<String, Object> memberDto(com.ww.runtime.common.NodeInfo info) {
        Map<String, Object> dto = nodeDto(info);
        double phi = membershipService.phiOf(info.id());
        dto.put("phi", Math.round(phi * 100.0d) / 100.0d);
        return dto;
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
        return ResponseEntity.ok(capabilityStateStore.get(node, capability)
                .map(RuntimeAdminController::capabilityDto).orElse(null));
    }

    /** 全量节点视图（含 DEAD 墓碑行）。 */
    @GetMapping("/nodes")
    public List<Map<String, Object>> nodes() {
        return nodeRegistry.listAll().stream().map(RuntimeAdminController::nodeDto).toList();
    }

    /**
     * S2（§3.12②）手动复活端点：{@code POST /rt/nodes/{id}/revive}。
     *
     * <p>目标为本节点：走 {@link NodeRuntime#reRegister()} 完整复活（register 重置纪元 →
     * 自检 → ACTIVE），语义与心跳自动重注册一致。目标为远端节点：仅做注册表重置回
     * JOINING（epoch=0）——真实复活动作（自检 + 归位 ACTIVE + 自报直证）必须由其自身
     * 进程执行（存活时心跳/自动重注册随即归位；确实已死则超时被启动孤儿清扫再置 DEAD），
     * 本端点绝不代写远端 ACTIVE，避免假存活污染伸缩视图。</p>
     */
    @PostMapping("/nodes/{nodeId}/revive")
    public ResponseEntity<Object> revive(@PathVariable("nodeId") String nodeId) {
        NodeId node;
        try {
            node = NodeId.of(nodeId);
        } catch (RuntimeException e) {
            return badRequest(e.getMessage());
        }
        com.ww.runtime.common.NodeInfo info = nodeRegistry.get(node).orElse(null);
        if (info == null) {
            return badRequest("node not found: " + node);
        }
        if (node.equals(self)) {
            if (info.state() != com.ww.runtime.common.NodeState.DEAD) {
                return ResponseEntity.ok(Map.of("nodeId", node.value(),
                        "result", "skipped", "state", info.state().name(),
                        "message", "node is not DEAD, nothing to revive"));
            }
            nodeRuntime.reRegister();
            return ResponseEntity.ok(Map.of("nodeId", node.value(), "result", "re-registered",
                    "state", "ACTIVE", "epoch", "reset"));
        }
        // 远端：重置回 JOINING（register upsert 语义：epoch=0），由其自身收敛
        nodeRegistry.register(com.ww.runtime.common.NodeInfo.of(info.id(), info.host(),
                info.grpcPort(), info.httpPort(), com.ww.runtime.common.NodeState.JOINING, 0L,
                info.weight(), info.resources()));
        return ResponseEntity.ok(Map.of("nodeId", node.value(), "result", "rejoining",
                "state", "JOINING",
                "message", "epoch reset; the node's own heartbeat/self-check will converge it, "
                        + "stale JOINING will be swept if it is truly dead"));
    }

    /** 节点信息扁平 DTO：对齐前端 NodeInfo 契约（nodeId 为字符串，不暴露内部 record）。 */
    private static Map<String, Object> nodeDto(com.ww.runtime.common.NodeInfo info) {
        Map<String, Object> dto = new LinkedHashMap<>();
        dto.put("nodeId", info.id().value());
        dto.put("host", info.host());
        dto.put("grpcPort", info.grpcPort());
        dto.put("httpPort", info.httpPort());
        dto.put("state", info.state().name());
        dto.put("stateEpoch", info.stateEpoch());
        dto.put("weight", info.weight());
        return dto;
    }

    /** 能力记录扁平 DTO：node 字段展开为 nodeId 字符串。 */
    private static Map<String, Object> capabilityDto(CapabilityRecord record) {
        Map<String, Object> dto = new LinkedHashMap<>();
        dto.put("nodeId", record.node().value());
        dto.put("type", record.type().name());
        dto.put("desired", record.desired().name());
        dto.put("actual", record.actual().name());
        dto.put("updatedAtMillis", record.updatedAtMillis());
        return dto;
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
