package com.ww.runtime.server.decommission;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.cluster.decommission.DecommissionRecord;
import com.ww.runtime.cluster.decommission.DecommissionState;
import com.ww.runtime.cluster.decommission.DecommissionStore;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.data.cache.CacheCapability;
import com.ww.runtime.data.cache.CacheGrpcClient;
import com.ww.runtime.data.storage.StorageCapability;
import com.ww.runtime.data.storage.StorageGrpcClient;
import com.ww.runtime.migration.MigrationTask;
import com.ww.runtime.migration.MigrationTaskStore;
import com.ww.runtime.placement.Placement;
import com.ww.runtime.placement.PlacementStore;
import com.ww.runtime.scheduler.DesiredStateWriter;

/**
 * 退役准入与预检服务：由 REST 端点调用，提供数据预检、启动退役、查询进度。
 */
public final class DecommissionService {

    private static final Logger LOG = LoggerFactory.getLogger(DecommissionService.class);

    private final NodeRegistry registry;
    private final CapabilityStateStore capabilityStore;
    private final PlacementStore placementStore;
    private final DesiredStateWriter desiredStateWriter;
    private final DecommissionStore decommissionStore;
    private final CacheCapability cacheCapability;
    private final CacheGrpcClient cacheGrpcClient;
    private final StorageCapability storageCapability;
    private final StorageGrpcClient storageGrpcClient;
    private final MigrationTaskStore migrationTaskStore;
    private final NodeId self;
    private final Clock clock;

    public DecommissionService(NodeRegistry registry,
                               CapabilityStateStore capabilityStore,
                               PlacementStore placementStore,
                               DesiredStateWriter desiredStateWriter,
                               DecommissionStore decommissionStore,
                               CacheCapability cacheCapability,
                               CacheGrpcClient cacheGrpcClient,
                               StorageCapability storageCapability,
                               StorageGrpcClient storageGrpcClient,
                               MigrationTaskStore migrationTaskStore,
                               NodeId self,
                               Clock clock) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.capabilityStore = Objects.requireNonNull(capabilityStore, "capabilityStore");
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.desiredStateWriter = Objects.requireNonNull(desiredStateWriter, "desiredStateWriter");
        this.decommissionStore = Objects.requireNonNull(decommissionStore, "decommissionStore");
        this.cacheCapability = Objects.requireNonNull(cacheCapability, "cacheCapability");
        this.cacheGrpcClient = Objects.requireNonNull(cacheGrpcClient, "cacheGrpcClient");
        this.storageCapability = Objects.requireNonNull(storageCapability, "storageCapability");
        this.storageGrpcClient = Objects.requireNonNull(storageGrpcClient, "storageGrpcClient");
        this.migrationTaskStore = Objects.requireNonNull(migrationTaskStore, "migrationTaskStore");
        this.self = Objects.requireNonNull(self, "self");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * 预检：纯读，无副作用。返回节点数据量、分区分布、warnings。
     */
    public PrecheckResult precheck(NodeId node) {
        Objects.requireNonNull(node, "node");
        NodeInfo nodeInfo = registry.get(node)
                .orElseThrow(() -> new DecommissionException(404, "node not found: " + node));

        List<CapabilityRecord> nodeRecords = capabilityStore.list().stream()
                .filter(r -> r.node().equals(node))
                .toList();

        List<CapabilityPrecheck> capabilities = new ArrayList<>();
        long totalCacheItems = 0;
        long totalStorageObjects = 0;
        int totalPrimaryPartitions = 0;
        boolean requiresForce = false;
        boolean degraded = nodeInfo.state() == NodeState.DEAD || nodeInfo.state() == NodeState.SUSPECTED;
        List<String> warnings = new ArrayList<>();

        if (degraded) {
            warnings.add("节点状态为 " + nodeInfo.state() + "，可能已不可达，数据可能丢失");
        }

        for (CapabilityRecord record : nodeRecords) {
            CapabilityType type = record.type();
            List<Placement> placements = placementStore.list(type.name());
            int primaries = (int) placements.stream()
                    .filter(p -> p.node().equals(node) && p.isPrimary())
                    .count();
            int replicas = (int) placements.stream()
                    .filter(p -> p.node().equals(node) && !p.isPrimary())
                    .count();
            totalPrimaryPartitions += primaries;

            Long itemCount = null;
            boolean reachable = true;
            if (record.actual() == CapabilityState.ACTIVE) {
                try {
                    itemCount = countItems(type, node);
                    if (type == CapabilityType.CACHE) {
                        totalCacheItems += itemCount;
                    } else if (type == CapabilityType.STORAGE) {
                        totalStorageObjects += itemCount;
                    }
                } catch (RuntimeException e) {
                    reachable = false;
                    LOG.debug("precheck count failed on {} for {}: {}", node, type, e.getMessage());
                }
            }

            boolean isLastNode = desiredStateWriter.isLastDesiredActiveNode(node, type);
            if (isLastNode && primaries > 0) {
                requiresForce = true;
                warnings.add(type + " 是该能力的最后活跃节点且持有 " + primaries + " 个主分区");
            }

            capabilities.add(new CapabilityPrecheck(
                    type.name(), record.desired().name(), record.actual().name(),
                    primaries, replicas, itemCount, reachable));
        }

        return new PrecheckResult(node.value(), nodeInfo.state().name(), capabilities,
                totalCacheItems, totalStorageObjects, totalPrimaryPartitions,
                warnings, requiresForce, degraded);
    }

    private long countItems(CapabilityType type, NodeId node) {
        if (type == CapabilityType.CACHE) {
            return node.equals(self)
                    ? cacheCapability.countKeys(null)
                    : cacheGrpcClient.countKeysStrict(node, null);
        } else if (type == CapabilityType.STORAGE) {
            return node.equals(self)
                    ? storageCapability.totalObjectCount()
                    : storageGrpcClient.countObjectsStrict(node, null);
        }
        return 0;
    }

    /**
     * 启动退役：幂等。已有非终态记录直接返回。
     */
    public DecommissionRecord start(NodeId node, boolean force) {
        Objects.requireNonNull(node, "node");
        registry.get(node)
                .orElseThrow(() -> new DecommissionException(404, "node not found: " + node));

        Optional<DecommissionRecord> existing = decommissionStore.get(node);
        if (existing.isPresent() && !existing.get().state().terminal()) {
            return existing.get();
        }

        List<CapabilityRecord> nodeRecords = capabilityStore.list().stream()
                .filter(r -> r.node().equals(node))
                .toList();

        for (CapabilityRecord record : nodeRecords) {
            if (record.desired() == CapabilityState.ACTIVE
                    && desiredStateWriter.isLastDesiredActiveNode(node, record.type())) {
                List<Placement> placements = placementStore.list(record.type().name());
                long primaries = placements.stream()
                        .filter(p -> p.node().equals(node) && p.isPrimary())
                        .count();
                if (primaries > 0 && !force) {
                    throw new DecommissionException(409,
                            "node " + node + " is the last active node for " + record.type()
                                    + " with " + primaries + " primary partitions; use force=true to discard data");
                }
            }
        }

        long now = clock.nowMillis();
        DecommissionRecord record = new DecommissionRecord(
                node, DecommissionState.DRAINING, force, null, now, now, null);
        decommissionStore.upsert(record);

        for (CapabilityRecord cr : nodeRecords) {
            if (cr.desired() == CapabilityState.ACTIVE) {
                try {
                    desiredStateWriter.setDesired(node, cr.type(), CapabilityState.DISABLED);
                } catch (IllegalStateTransitionException e) {
                    decommissionStore.setState(node, DecommissionState.FAILED, e.getMessage());
                    throw new DecommissionException(409, "failed to set desired=DISABLED: " + e.getMessage());
                }
            }
        }

        LOG.info("decommission started for node {} (force={})", node, force);
        return record;
    }

    public Optional<DecommissionRecord> status(NodeId node) {
        Objects.requireNonNull(node, "node");
        return decommissionStore.get(node);
    }

    /**
     * 查询退役进度（状态 + 各能力剩余分区 + 活跃迁移任务数）。
     */
    public ProgressResult progress(NodeId node) {
        Objects.requireNonNull(node, "node");
        Optional<DecommissionRecord> record = decommissionStore.get(node);
        if (record.isEmpty()) {
            return new ProgressResult(null, List.of(), 0, 0);
        }

        List<CapabilityRecord> nodeRecords = capabilityStore.list().stream()
                .filter(r -> r.node().equals(node))
                .toList();

        List<CapabilityProgress> progress = new ArrayList<>();
        int totalPartitionsLeft = 0;
        for (CapabilityRecord cr : nodeRecords) {
            List<Placement> placements = placementStore.list(cr.type().name());
            int primariesLeft = (int) placements.stream()
                    .filter(p -> p.node().equals(node) && p.isPrimary())
                    .count();
            int replicasLeft = (int) placements.stream()
                    .filter(p -> p.node().equals(node) && !p.isPrimary())
                    .count();
            totalPartitionsLeft += primariesLeft + replicasLeft;

            long activeMigrations = migrationTaskStore.listActive().stream()
                    .filter(t -> node.equals(t.fromNode()) && cr.type().name().equals(t.capability()))
                    .count();

            progress.add(new CapabilityProgress(
                    cr.type().name(), cr.desired().name(), cr.actual().name(),
                    primariesLeft, replicasLeft, activeMigrations));
        }

        return new ProgressResult(record.get(), progress, totalPartitionsLeft,
                record.get().state().terminal() ? 0 : totalPartitionsLeft);
    }

    // -- DTO records --

    public record PrecheckResult(String nodeId, String nodeState,
                                  List<CapabilityPrecheck> capabilities,
                                  long totalCacheItems, long totalStorageObjects,
                                  int totalPrimaryPartitions,
                                  List<String> warnings, boolean requiresForce,
                                  boolean degraded) {
    }

    public record CapabilityPrecheck(String type, String desired, String actual,
                                      int primaryPartitions, int replicaPartitions,
                                      Long itemCount, boolean reachable) {
    }

    public record ProgressResult(DecommissionRecord record,
                                  List<CapabilityProgress> capabilities,
                                  int totalPartitionsLeft,
                                  int totalPartitionsEstimated) {
    }

    public record CapabilityProgress(String type, String desired, String actual,
                                      int primaryPartitionsLeft, int replicaPartitionsLeft,
                                      long activeMigrationsFromNode) {
    }

    /** 退役业务异常（携带 HTTP 状态码）。 */
    public static final class DecommissionException extends RuntimeException {
        private final int httpStatus;

        public DecommissionException(int httpStatus, String message) {
            super(message);
            this.httpStatus = httpStatus;
        }

        public int httpStatus() {
            return httpStatus;
        }
    }
}
