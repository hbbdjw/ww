package com.ww.runtime.server.web;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.data.cache.CacheCapability;
import com.ww.runtime.data.cache.CacheGrpcClient;
import com.ww.runtime.data.storage.StorageCapability;
import com.ww.runtime.data.storage.StorageGrpcClient;
import com.ww.runtime.migration.MigrationEngine;
import com.ww.runtime.migration.MigrationTask;
import com.ww.runtime.migration.MigrationTaskStore;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.scheduler.LeaseStore;
import com.ww.runtime.scheduler.ReconcileLoop;

/**
 * 调度管理端点（前缀 {@code /rt}）：缓存调度操作面——迁移任务列表/取消/重试、
 * 手动触发重平衡、放置分布与每节点 cache 统计。
 *
 * <p>与 {@link RuntimeAdminController}（集群状态/期望态）分工：本控制器承载调度
 * 过程的可观察与干预；全部端点为管理面低频调用。错误约定与其一致：
 * 400 + {@code {message}}（非法参数），404（任务不存在）。</p>
 *
 * <ul>
 *   <li><b>GET /rt/migration/tasks</b>：任务列表（capability/scope/limit 过滤，
 *       taskId 降序，有界 + truncated 标志）；</li>
 *   <li><b>POST /rt/migration/tasks/{taskId}/cancel</b>：取消非终态任务（幂等：
 *       终态/切主后返回 cancelled=false）；</li>
 *   <li><b>POST /rt/migration/resume</b>：重试失败/中断迁移（FAILED→PENDING 断点续跑）；</li>
 *   <li><b>POST /rt/scheduler/rebalance</b>：重置重平衡冷却（leader 亲和：响应带
 *       isLeader/leader 供调用方判断有效性）；</li>
 *   <li><b>GET /rt/placement/{capability}/distribution</b>：每节点主/副本分区数聚合；</li>
 *   <li><b>GET /rt/cache/node-stats</b>：每节点 cache 条目统计（strict 计数区分不可达）；</li>
 *   <li><b>GET /rt/storage/node-stats</b>：每节点存储对象统计（strict 计数区分不可达，
 *       与缓存同约定）。</li>
 * </ul>
 */
@RestController
@RequestMapping("/rt")
public class RuntimeScheduleController {

    private static final Logger LOG = LoggerFactory.getLogger(RuntimeScheduleController.class);

    /** 任务列表单次上限（管理面防御：scope=all 时重平衡可产生数千终态任务）。 */
    private static final int MAX_TASKS_LIMIT = 500;
    private static final int DEFAULT_TASKS_LIMIT = 100;

    /** checkpoint_json 中 sourceCount 的轻量解析（与 MigrationEngine.Meta 同族口径）。 */
    private static final Pattern SOURCE_COUNT_PATTERN =
            Pattern.compile("\"sourceCount\"\\s*:\\s*(-?\\d+)");

    private final MigrationTaskStore migrationTaskStore;
    private final MigrationEngine migrationEngine;
    private final ReconcileLoop reconcileLoop;
    private final LeaseStore leaseStore;
    private final PlacementStore placementStore;
    private final CapabilityStateStore capabilityStateStore;
    private final CacheCapability cacheCapability;
    private final CacheGrpcClient cacheGrpcClient;
    private final StorageCapability storageCapability;
    private final StorageGrpcClient storageGrpcClient;
    private final NodeId self;

    public RuntimeScheduleController(MigrationTaskStore migrationTaskStore,
                                     MigrationEngine migrationEngine,
                                     ReconcileLoop reconcileLoop,
                                     LeaseStore leaseStore,
                                     PlacementStore placementStore,
                                     CapabilityStateStore capabilityStateStore,
                                     CacheCapability cacheCapability,
                                     CacheGrpcClient cacheGrpcClient,
                                     StorageCapability storageCapability,
                                     StorageGrpcClient storageGrpcClient,
                                     NodeId self) {
        this.migrationTaskStore = Objects.requireNonNull(migrationTaskStore, "migrationTaskStore");
        this.migrationEngine = Objects.requireNonNull(migrationEngine, "migrationEngine");
        this.reconcileLoop = Objects.requireNonNull(reconcileLoop, "reconcileLoop");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.capabilityStateStore = Objects.requireNonNull(capabilityStateStore, "capabilityStateStore");
        this.cacheCapability = Objects.requireNonNull(cacheCapability, "cacheCapability");
        this.cacheGrpcClient = Objects.requireNonNull(cacheGrpcClient, "cacheGrpcClient");
        this.storageCapability = Objects.requireNonNull(storageCapability, "storageCapability");
        this.storageGrpcClient = Objects.requireNonNull(storageGrpcClient, "storageGrpcClient");
        this.self = Objects.requireNonNull(self, "self");
    }

    // ------------------------------------------------------------------
    // 迁移任务
    // ------------------------------------------------------------------

    /**
     * 迁移任务列表：按 taskId 降序（最新在前）。
     *
     * @param capability 能力过滤（可选，缺省全部）
     * @param scope      {@code active}（默认，非终态）或 {@code all}（含终态）
     * @param limit      返回上限（默认 100，最大 500）
     */
    @GetMapping("/migration/tasks")
    public Map<String, Object> migrationTasks(
            @RequestParam(value = "capability", required = false) String capability,
            @RequestParam(value = "scope", defaultValue = "active") String scope,
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        String capFilter = capability == null || capability.isBlank()
                ? null : capability.trim().toUpperCase(Locale.ROOT);
        boolean all = "all".equalsIgnoreCase(scope);
        int capped = Math.max(1, Math.min(limit > 0 ? limit : DEFAULT_TASKS_LIMIT, MAX_TASKS_LIMIT));

        List<MigrationTask> source = all ? migrationTaskStore.listAll()
                : migrationTaskStore.listActive();
        List<MigrationTask> filtered = capFilter == null
                ? new ArrayList<>(source)
                : source.stream().filter(t -> t.capability().equals(capFilter)).toList();

        List<MigrationTaskDto> tasks = filtered.stream()
                .sorted(Comparator.comparingLong(MigrationTask::taskId).reversed())
                .limit(capped)
                .map(RuntimeScheduleController::toDto)
                .toList();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total", filtered.size());
        body.put("truncated", filtered.size() > tasks.size());
        body.put("tasks", tasks);
        return body;
    }

    /**
     * 取消迁移任务（幂等）：仅非终态任务可取消；CUTOVER 之后状态机拒绝
     * （切主已生效不可逆）→ cancelled=false。
     */
    @PostMapping("/migration/tasks/{taskId}/cancel")
    public ResponseEntity<Object> cancelTask(@PathVariable("taskId") long taskId) {
        try {
            boolean cancelled = migrationEngine.cancel(taskId);
            String message = cancelled
                    ? "task " + taskId + " cancelled"
                    : "task " + taskId + " already terminal (cannot cancel)";
            return ResponseEntity.ok(Map.of("cancelled", cancelled, "message", message));
        } catch (com.ww.runtime.common.MigrationException e) {
            return ResponseEntity.status(404).body(Map.of("message", e.getMessage()));
        } catch (RuntimeException e) {
            // CAS 并发冲突等：任务可能已被并发推进/取消——按未取消语义返回
            LOG.warn("cancel task {} failed: {}", taskId, e.getMessage());
            return ResponseEntity.ok(Map.of("cancelled", false,
                    "message", "cancel contended: " + e.getMessage()));
        }
    }

    /**
     * 重试失败/中断迁移：FAILED 任务经 FAILED→PENDING 跃迁后按断点续跑
     * （多节点并发执行安全——引擎 CAS 争抢保护）。幂等。
     */
    @PostMapping("/migration/resume")
    public Map<String, Object> resumeMigrations() {
        migrationEngine.resumeAll();
        return Map.of("resumed", true);
    }

    // ------------------------------------------------------------------
    // 重平衡触发
    // ------------------------------------------------------------------

    /**
     * 手动触发重平衡：重置该能力的重平衡冷却，leader 实例下个 reconcile 周期
     * （≤ reconcileIntervalMs）重新规划（minMoveRatio 门控仍生效）。
     *
     * <p><b>leader 亲和</b>：冷却为本节点实例内存态——请求打到非 leader 节点时
     * reset 无实际效果；响应带 isLeader/leader 供调用方提示（重复调用无害）。</p>
     */
    @PostMapping("/scheduler/rebalance")
    public ResponseEntity<Object> triggerRebalance(@RequestBody RebalanceRequest body) {
        String raw = body == null ? null : body.capability();
        CapabilityType type;
        try {
            type = CapabilityType.parse(String.valueOf(raw));
        } catch (RuntimeException e) {
            return badRequest("unknown capability: " + raw);
        }
        reconcileLoop.resetRebalanceCooldown(type);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("reset", true);
        response.put("isLeader", reconcileLoop.isLeader());
        response.put("leader", leaseStore.currentOwner().orElse(null));
        return ResponseEntity.ok(response);
    }

    // ------------------------------------------------------------------
    // 放置分布
    // ------------------------------------------------------------------

    /**
     * 放置分布：该能力每节点的 PRIMARY/REPLICA 分区数（按 primaries 降序）。
     * 服务端单次全量读 + 聚合——不进 /rt/status SSE（每秒推送会放大 payload）。
     */
    @GetMapping("/placement/{capability}/distribution")
    public ResponseEntity<Object> distribution(@PathVariable("capability") String capability) {
        CapabilityType type;
        try {
            type = CapabilityType.parse(capability);
        } catch (RuntimeException e) {
            return badRequest(e.getMessage());
        }
        List<Placement> placements = placementStore.list(type.name());
        Map<String, int[]> counts = new LinkedHashMap<>(); // nodeId → [primaries, replicas]
        for (Placement placement : placements) {
            int[] c = counts.computeIfAbsent(placement.node().value(),
                    k -> new int[2]);
            if (placement.isPrimary()) {
                c[0]++;
            } else {
                c[1]++;
            }
        }
        List<Map<String, Object>> distribution = counts.entrySet().stream()
                .sorted(Map.Entry.<String, int[]>comparingByValue(
                                Comparator.comparingInt((int[] c) -> c[0]).reversed())
                        .thenComparing(Map.Entry.comparingByKey()))
                .map(e -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("nodeId", e.getKey());
                    row.put("primaries", e.getValue()[0]);
                    row.put("replicas", e.getValue()[1]);
                    return row;
                })
                .toList();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("capability", type.name());
        body.put("partitionCount", placements.stream().filter(Placement::isPrimary).count());
        body.put("totalPlacements", placements.size());
        body.put("distribution", distribution);
        return ResponseEntity.ok(body);
    }

    // ------------------------------------------------------------------
    // 每节点 cache 统计
    // ------------------------------------------------------------------

    /**
     * 每节点 cache 统计：actual=ACTIVE 的节点取条目数——self 走本地引擎，
     * peer 走 gRPC strict 计数（失败标记 reachable=false，与「0 条目」区分）。
     * 单 peer 失败不影响其他行（per-node 错误隔离）。
     */
    @GetMapping("/cache/node-stats")
    public Map<String, Object> cacheNodeStats() {
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<CapabilityStateStore.CapabilityRecord> cacheRecords =
                capabilityStateStore.list().stream()
                        .filter(r -> r.type() == CapabilityType.CACHE)
                        .sorted(Comparator.comparing(r -> r.node().value()))
                        .toList();
        for (CapabilityStateStore.CapabilityRecord record : cacheRecords) {
            NodeId node = record.node();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("nodeId", node.value());
            row.put("cacheActual", record.actual().name());
            Long keyCount = null;
            boolean reachable = true;
            if (record.actual() == CapabilityState.ACTIVE) {
                try {
                    keyCount = node.equals(self)
                            ? localKeyCount()
                            : cacheGrpcClient.countKeysStrict(node, null);
                } catch (RuntimeException e) {
                    reachable = false;
                    LOG.debug("cache node-stats failed on {}: {}", node, e.getMessage());
                }
            }
            row.put("keyCount", keyCount);
            row.put("reachable", reachable);
            nodes.add(row);
        }
        return Map.of("nodes", nodes);
    }

    private long localKeyCount() {
        try {
            return cacheCapability.countKeys(null);
        } catch (RuntimeException e) {
            // actual=ACTIVE 但能力未启动（瞬时状态机竞态）——按不可达语义
            throw new IllegalStateException("local cache capability not started", e);
        }
    }

    // ------------------------------------------------------------------
    // 每节点存储统计
    // ------------------------------------------------------------------

    /**
     * 每节点存储统计：actual=ACTIVE 的节点取对象数——self 走本地引擎，
     * peer 走 gRPC strict 计数（失败标记 reachable=false，与「0 对象」区分）。
     * 单 peer 失败不影响其他行（per-node 错误隔离，与缓存同约定）。
     */
    @GetMapping("/storage/node-stats")
    public Map<String, Object> storageNodeStats() {
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<CapabilityStateStore.CapabilityRecord> storageRecords =
                capabilityStateStore.list().stream()
                        .filter(r -> r.type() == CapabilityType.STORAGE)
                        .sorted(Comparator.comparing(r -> r.node().value()))
                        .toList();
        for (CapabilityStateStore.CapabilityRecord record : storageRecords) {
            NodeId node = record.node();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("nodeId", node.value());
            row.put("storageActual", record.actual().name());
            Long objectCount = null;
            boolean reachable = true;
            if (record.actual() == CapabilityState.ACTIVE) {
                try {
                    objectCount = node.equals(self)
                            ? localObjectCount()
                            : storageGrpcClient.countObjectsStrict(node, null);
                } catch (RuntimeException e) {
                    reachable = false;
                    LOG.debug("storage node-stats failed on {}: {}", node, e.getMessage());
                }
            }
            row.put("objectCount", objectCount);
            row.put("reachable", reachable);
            nodes.add(row);
        }
        return Map.of("nodes", nodes);
    }

    private long localObjectCount() {
        try {
            return storageCapability.totalObjectCount();
        } catch (RuntimeException e) {
            // actual=ACTIVE 但能力未启动（瞬时状态机竞态）——按不可达语义
            throw new IllegalStateException("local storage capability not started", e);
        }
    }

    // ------------------------------------------------------------------
    // DTO
    // ------------------------------------------------------------------

    /** 迁移任务扁平 DTO（NodeId record 不直接序列化，与 capabilityDto 同约定）。 */
    private static MigrationTaskDto toDto(MigrationTask task) {
        return new MigrationTaskDto(task.taskId(), task.capability(), task.partition(),
                task.fromNode() == null ? null : task.fromNode().value(),
                task.toNode().value(), task.state().name(),
                task.checkpointOffset(), task.itemsDone(),
                parseSourceCount(task.checkpointJson()), task.updatedAtMillis());
    }

    /** 从 checkpoint_json 提取 sourceCount（无则 -1——SNAPSHOT 前未知）。 */
    private static long parseSourceCount(String checkpointJson) {
        if (checkpointJson == null || checkpointJson.isBlank()) {
            return -1L;
        }
        Matcher matcher = SOURCE_COUNT_PATTERN.matcher(checkpointJson);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : -1L;
    }

    /**
     * @param taskId        任务 id
     * @param capability    能力标识
     * @param partition     分区号
     * @param fromNode      源节点（引导型新分区为 null）
     * @param toNode        目标节点
     * @param state         任务状态（10 态）
     * @param checkpointOffset 断点偏移（条目数）
     * @param itemsDone     已完成条目数
     * @param sourceCount   源端总条目数（checkpoint_json 解析，未知为 -1）
     * @param updatedAtMillis 最后更新时间
     */
    public record MigrationTaskDto(long taskId, String capability, int partition,
                                   String fromNode, String toNode, String state,
                                   long checkpointOffset, long itemsDone,
                                   long sourceCount, long updatedAtMillis) {
    }

    /** 重平衡触发请求体。 */
    public record RebalanceRequest(String capability) {
    }

    private static ResponseEntity<Object> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("message", String.valueOf(message)));
    }
}
