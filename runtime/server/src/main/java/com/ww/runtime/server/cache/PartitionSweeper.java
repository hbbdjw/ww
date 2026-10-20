package com.ww.runtime.server.cache;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ww.runtime.common.MigrationTaskState;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.data.CompositePartitionAdapter;
import com.ww.runtime.migration.MigrationTask;
import com.ww.runtime.migration.MigrationTaskStore;
import com.ww.runtime.placement.PlacementStore;

/**
 * 启动清扫器（能力无关）：节点启动、放置表引导完成后，对本机各能力数据做
 * 「归属裁剪」——保留本节点仍是 PRIMARY 的分区与仍在途（非终态或 FAILED 可重试）
 * 迁移任务涉及的分区，删除其余分区数据，并触发持久化压实。
 *
 * <p>解决两类残留：① 崩溃恢复/AOF 回放出的旧归属数据（分区已迁走，本节点不再持有）；
 * ② 迁移后 clearPartition 失败的兜底清理。放置表（PG 持久）为唯一路由真源。</p>
 *
 * <h2>keep 规则（安全优先）</h2>
 * <pre>keep(p) = self 是 p 的 PRIMARY ‖ ∃任务(capability, p)：self ∈ {from, to} 且 state ∉ {COMPLETED, CANCELLED}
 *           ‖ p 处于 DEGRADED 隔离（P0-2：最后副本在死亡属主页上，删除即永久丢失）</pre>
 * <ul>
 *   <li>FAILED 任务必须保留：FAILED→PENDING 会断点续跑，误删部分导入数据将导致对账失败；</li>
 *   <li>COMPLETED/CANCELLED 任务的源端数据可删（目标端已对账/任务已放弃）；</li>
 *   <li>P0-2 隔离分区必须保留：放置仍指向死亡属主（押注重启），此时本节点磁盘上的
 *       数据是最后副本——旧实现在此会把它当"非归属残留"删除，将暂时不可用转化为
 *       永久销毁；属主回归或 force-adopt 后由后续 sweep 自然清理；</li>
 *   <li>放置表为空（能力未引导）→ 跳过该能力——无法判定归属时绝不删除。</li>
 * </ul>
 *
 * <p>能力未实现清扫（retainPartitions default 返回 0）或未启动时自然 no-op；
 * 单能力异常记日志跳过，不影响其他能力与启动流程。</p>
 */
public final class PartitionSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionSweeper.class);

    private final CompositePartitionAdapter adapter;
    private final PlacementStore placementStore;
    private final MigrationTaskStore taskStore;
    /** P0-2 隔离注册表（可空：未装配时不追加隔离保留规则）。 */
    private final com.ww.runtime.placement.DegradedPartitionStore degradedStore;
    private final NodeId self;

    public PartitionSweeper(CompositePartitionAdapter adapter,
                            PlacementStore placementStore,
                            MigrationTaskStore taskStore,
                            NodeId self) {
        this(adapter, placementStore, taskStore, self, null);
    }

    public PartitionSweeper(CompositePartitionAdapter adapter,
                            PlacementStore placementStore,
                            MigrationTaskStore taskStore,
                            NodeId self,
                            com.ww.runtime.placement.DegradedPartitionStore degradedStore) {
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.taskStore = Objects.requireNonNull(taskStore, "taskStore");
        this.self = Objects.requireNonNull(self, "self");
        this.degradedStore = degradedStore;
    }

    /** 遍历组合适配器注册的全部能力逐个清扫（单能力异常跳过）。 */
    public void sweepAll() {
        for (String capability : adapter.capabilities()) {
            try {
                sweep(capability);
            } catch (RuntimeException e) {
                LOG.warn("partition sweep failed for {} on {}: {}",
                        capability, self, e.getMessage());
            }
        }
    }

    private void sweep(String capability) {
        Map<Integer, NodeId> primaries = placementStore.primaries(capability);
        if (primaries.isEmpty()) {
            LOG.debug("sweep skipped for {}: no placement ring yet", capability);
            return; // 放置表未引导：无法判定归属，绝不删除
        }
        Set<Integer> taskPartitions = partitionsOfActiveTasksInvolvingSelf(capability);
        // P0-2：隔离分区是死亡属主页上的最后副本，绝不按"非归属残留"删除
        Set<Integer> degraded = degradedStore != null
                ? degradedStore.partitionsOf(capability) : Set.of();
        long removed = adapter.retainPartitions(capability,
                p -> self.equals(primaries.get(p)) || taskPartitions.contains(p)
                        || degraded.contains(p));
        if (removed > 0) {
            LOG.info("swept {} stale entries of {} on {} (non-owned, no active task), compacting storage",
                    removed, capability, self);
            adapter.compact(capability);
        }
    }

    /** 涉及本节点且未终结（含 FAILED 可重试）的迁移任务分区集合。 */
    private Set<Integer> partitionsOfActiveTasksInvolvingSelf(String capability) {
        Set<Integer> partitions = new HashSet<>();
        for (MigrationTask task : taskStore.listAll()) {
            if (!task.capability().equals(capability)) {
                continue;
            }
            if (task.state() == MigrationTaskState.COMPLETED
                    || task.state() == MigrationTaskState.CANCELLED) {
                continue;
            }
            if (self.equals(task.fromNode()) || self.equals(task.toNode())) {
                partitions.add(task.partition());
            }
        }
        return partitions;
    }
}
