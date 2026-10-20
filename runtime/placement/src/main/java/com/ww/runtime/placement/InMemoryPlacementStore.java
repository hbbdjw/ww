package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;
import com.ww.runtime.common.RuntimeFrameworkException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 进程内 {@link PlacementStore} 实现：线程安全。
 *
 * <p>每个 capability 持有一个不可变 {@link Snapshot}，replaceAll 构建完整
 * 快照后单次替换引用（原子整体替换）；读方法拿到快照引用后读取，天然一致。
 * cutover / promoteReplica / reassignReplicas 在 {@link ConcurrentHashMap#compute}
 * 的原子语义内完成「校验 + 派生新快照」的 CAS。</p>
 *
 * <p><b>主键对齐：</b>cutover 与 PostgreSQL 实现（rt_partition_placement 主键
 * {@code (capability, partition_id, node_id)}）同语义——目标节点已持有该分区任意行时
 * 改写会违反唯一约束，抛 {@link RuntimeFrameworkException}（而非静默成功），
 * 保证内存实现与 JDBC 实现行为一致。</p>
 */
public final class InMemoryPlacementStore implements PlacementStore {

    /** 一个 capability 的不可变放置快照（多行派生视图一并固化）。 */
    private record Snapshot(List<Placement> placements,
                            Map<Integer, List<Placement>> byPartition,
                            Map<Integer, NodeId> primaries) {
    }

    private final ConcurrentHashMap<String, Snapshot> byCapability = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> revisions = new ConcurrentHashMap<>();

    @Override
    public void replaceAll(String capability, List<Placement> placements) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(placements, "placements");
        byCapability.put(capability, snapshotOf(placements));
        bumpRevision(capability);
    }

    @Override
    public long revision(String capability) {
        Objects.requireNonNull(capability, "capability");
        AtomicLong revision = revisions.get(capability);
        return revision == null ? 0L : revision.get();
    }

    @Override
    public Map<Integer, Long> renewLeases(String capability, List<Integer> partitions,
                                          NodeId owner, long epoch, long ttlMillis) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(partitions, "partitions");
        Objects.requireNonNull(owner, "owner");
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be > 0, got " + ttlMillis);
        }
        Snapshot snapshot = byCapability.get(capability);
        if (snapshot == null || partitions.isEmpty()) {
            return Map.of();
        }
        long now = System.currentTimeMillis();
        long leaseUntil = now + ttlMillis;
        Map<Integer, Long> renewed = new LinkedHashMap<>();
        for (Integer partition : partitions) {
            Placement primary = primaryOf(snapshot, partition);
            if (primary != null && primary.node().equals(owner) && primary.epoch() == epoch
                    && primary.validAfterMs() <= now) {
                renewed.put(partition, leaseUntil);
            }
        }
        return renewed;
    }

    @Override
    public void releaseLeases(String capability, List<Integer> partitions, NodeId owner) {
        // 进程内实现没有持久租约；节点停止即不再写入。保留接口语义。
    }

    private void bumpRevision(String capability) {
        revisions.computeIfAbsent(capability, ignored -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public List<Placement> list(String capability) {
        Objects.requireNonNull(capability, "capability");
        Snapshot snapshot = byCapability.get(capability);
        return snapshot == null ? List.of() : snapshot.placements();
    }

    @Override
    public Optional<Placement> get(String capability, int partition) {
        List<Placement> rows = placements(capability, partition);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    @Override
    public List<Placement> placements(String capability, int partition) {
        Objects.requireNonNull(capability, "capability");
        Snapshot snapshot = byCapability.get(capability);
        List<Placement> rows = snapshot == null ? null : snapshot.byPartition().get(partition);
        return rows == null ? List.of() : rows;
    }

    @Override
    public Map<Integer, NodeId> primaries(String capability) {
        Objects.requireNonNull(capability, "capability");
        Snapshot snapshot = byCapability.get(capability);
        return snapshot == null ? Map.of() : snapshot.primaries();
    }

    @Override
    public boolean cutover(String capability, int partition, NodeId newNode, long expectedEpoch) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(newNode, "newNode");
        AtomicReference<Boolean> success = new AtomicReference<>(Boolean.FALSE);
        byCapability.compute(capability, (cap, snapshot) -> {
            if (snapshot == null) {
                return null;
            }
            Placement current = primaryOf(snapshot, partition);
            if (current == null || current.epoch() != expectedEpoch || current.node().equals(newNode)) {
                return snapshot;
            }
            // 主键 (capability, partition_id, node_id) 唯一性对齐：目标节点已持有该分区行
            //（典型为 REPLICA——排空场景中目标必为旧环第二顺位=副本持有者）时，
            // PG 的 UPDATE 直接抛 duplicate key，内存版同样抛错而非静默改写。
            if (replicaOf(snapshot, partition, newNode) != null) {
                throw new RuntimeFrameworkException("InMemoryPlacementStore.cutover failed for capability "
                        + capability + " partition " + partition
                        + ": node " + newNode + " already holds a placement row (duplicate key)");
            }
            success.set(Boolean.TRUE);
            Placement updated = new Placement(partition, newNode, PlacementRole.PRIMARY, current.epoch() + 1);
            return withReplacedPrimary(snapshot, current,
                    withPrimaryAndReplicas(updated, snapshot.byPartition().get(partition)));
        });
        if (success.get()) {
            bumpRevision(capability);
        }
        return success.get();
    }

    @Override
    public boolean promoteReplica(String capability, int partition, NodeId newNode, long expectedEpoch) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(newNode, "newNode");
        AtomicReference<Boolean> success = new AtomicReference<>(Boolean.FALSE);
        byCapability.compute(capability, (cap, snapshot) -> {
            if (snapshot == null) {
                return null;
            }
            Placement primary = primaryOf(snapshot, partition);
            if (primary == null || primary.epoch() != expectedEpoch) {
                return snapshot;
            }
            Placement replica = replicaOf(snapshot, partition, newNode);
            if (replica == null) {
                return snapshot;
            }
            success.set(Boolean.TRUE);
            Placement promoted = new Placement(partition, newNode, PlacementRole.PRIMARY, primary.epoch() + 1);
            return withReplacedPrimary(snapshot, primary,
                    withPrimaryAndReplicas(promoted, snapshot.byPartition().get(partition)));
        });
        if (success.get()) {
            bumpRevision(capability);
        }
        return success.get();
    }

    @Override
    public void reassignReplicas(String capability, int partition, List<NodeId> newReplicas) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(newReplicas, "newReplicas");
        AtomicBoolean changed = new AtomicBoolean(false);
        byCapability.compute(capability, (cap, snapshot) -> {
            if (snapshot == null) {
                return null;
            }
            Placement primary = primaryOf(snapshot, partition);
            if (primary == null) {
                return snapshot; // 分区不存在：无主可依附，忽略
            }
            Set<NodeId> distinct = new LinkedHashSet<>(newReplicas);
            distinct.remove(primary.node()); // 主节点不兼任副本
            List<Placement> rows = new ArrayList<>(1 + distinct.size());
            rows.add(primary);
            for (NodeId node : distinct) {
                rows.add(new Placement(partition, node, PlacementRole.REPLICA, primary.epoch()));
            }
            changed.set(true);
            return withReplacedPrimary(snapshot, primary, rows);
        });
        if (changed.get()) {
            bumpRevision(capability);
        }
    }

    private static Placement primaryOf(Snapshot snapshot, int partition) {
        List<Placement> rows = snapshot.byPartition().get(partition);
        return rows == null || rows.isEmpty() ? null : rows.getFirst();
    }

    /**
     * 组装分区新行集：newPrimary 在 [0]，原行集中非主且非 newPrimary 节点的
     * REPLICA 行保留（cutover/promote 不破坏副本集；newPrimary 兼任旧副本的行
     * 由升主/迁主行替代）。
     */
    private static List<Placement> withPrimaryAndReplicas(Placement newPrimary, List<Placement> oldRows) {
        List<Placement> rows = new ArrayList<>(oldRows.size());
        rows.add(newPrimary);
        if (oldRows != null) {
            for (Placement p : oldRows) {
                if (!p.isPrimary() && !p.node().equals(newPrimary.node())) {
                    rows.add(p);
                }
            }
        }
        return rows;
    }

    private static Placement replicaOf(Snapshot snapshot, int partition, NodeId node) {
        List<Placement> rows = snapshot.byPartition().get(partition);
        if (rows == null) {
            return null;
        }
        for (int i = 1; i < rows.size(); i++) {
            if (rows.get(i).node().equals(node)) {
                return rows.get(i);
            }
        }
        return null;
    }

    private static Snapshot snapshotOf(List<Placement> placements) {
        Map<Integer, List<Placement>> grouped = new LinkedHashMap<>();
        for (Placement placement : placements) {
            Objects.requireNonNull(placement, "placement");
            grouped.computeIfAbsent(placement.partition(), p -> new ArrayList<>()).add(placement);
        }
        List<Placement> sorted = new ArrayList<>(placements.size());
        Map<Integer, List<Placement>> byPartition = new LinkedHashMap<>();
        Map<Integer, NodeId> primaries = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<Placement>> entry : grouped.entrySet()) {
            int partition = entry.getKey();
            List<Placement> rows = new ArrayList<>(entry.getValue());
            rows.sort(Comparator.comparing(Placement::isPrimary).reversed()
                    .thenComparing(p -> p.node().value()));
            long primaryCount = rows.stream().filter(Placement::isPrimary).count();
            if (primaryCount > 1) {
                throw new IllegalArgumentException(
                        "duplicate PRIMARY placement for partition " + partition);
            }
            if (primaryCount == 1) {
                primaries.put(partition, rows.getFirst().node());
            }
            List<Placement> frozen = List.copyOf(rows);
            byPartition.put(partition, frozen);
            sorted.addAll(frozen);
        }
        return new Snapshot(List.copyOf(sorted),
                Collections.unmodifiableMap(byPartition),
                Collections.unmodifiableMap(primaries));
    }

    /** 以 rows 整体替换 oldPrimary 所在分区的行集，派生新快照。 */
    private static Snapshot withReplacedPrimary(Snapshot snapshot, Placement oldPrimary, List<Placement> rows) {
        List<Placement> replaced = new ArrayList<>(snapshot.placements().size());
        for (Placement placement : snapshot.placements()) {
            if (placement.partition() != oldPrimary.partition()) {
                replaced.add(placement);
            }
        }
        replaced.addAll(rows);
        return snapshotOf(replaced);
    }
}
