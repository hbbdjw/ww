package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 再均衡规划器：对比当前与目标 primary 分布，产出最小差集迁移计划，
 * 并按移动比例阈值 + 冷却时间决定是否触发再均衡。
 */
public final class RebalancePlanner {

    /**
     * 单个分区的 primary 迁移：{@code from} 可为 null（目标集新增的分区，无历史归属）。
     */
    public record PartitionMove(String capability, int partition, NodeId from, NodeId to) {

        public PartitionMove {
            Objects.requireNonNull(capability, "capability");
            Objects.requireNonNull(to, "to");
            if (partition < 0) {
                throw new IllegalArgumentException("partition must be >= 0, got " + partition);
            }
        }
    }

    /**
     * 计算从 {@code currentPrimaries} 到 {@code targetPrimaries} 的差集迁移计划。
     *
     * <p>仅输出 primary 发生变化的分区（最小移动语义）；目标集中存在而当前集
     * 缺失的分区视为新增，from 为 null。结果按 partition 升序。</p>
     */
    public List<PartitionMove> plan(String capability,
                                    Map<Integer, NodeId> currentPrimaries,
                                    Map<Integer, NodeId> targetPrimaries) {
        Objects.requireNonNull(capability, "capability");
        Objects.requireNonNull(currentPrimaries, "currentPrimaries");
        Objects.requireNonNull(targetPrimaries, "targetPrimaries");
        List<PartitionMove> moves = new ArrayList<>();
        for (Map.Entry<Integer, NodeId> entry : targetPrimaries.entrySet()) {
            int partition = entry.getKey();
            NodeId target = Objects.requireNonNull(entry.getValue(), "targetPrimaries value");
            NodeId current = currentPrimaries.get(partition);
            if (!target.equals(current)) {
                moves.add(new PartitionMove(capability, partition, current, target));
            }
        }
        moves.sort(Comparator.comparingInt(PartitionMove::partition));
        return moves;
    }

    /**
     * 是否应触发再均衡。
     *
     * <p>moved/total &lt; minMoveRatio（移动量不足，不值得切换）或
     * {@code nowMs - lastRunMs < cooldownMs}（仍在冷却期内）时返回 false；
     * 比例恰好等于阈值、冷却恰好耗尽均视为可执行。totalPartitions &lt;= 0 无意义，返回 false。</p>
     */
    public boolean shouldRebalance(int movedCount, int totalPartitions, double minMoveRatio,
                                   long lastRunMs, long cooldownMs, long nowMs) {
        if (totalPartitions <= 0) {
            return false;
        }
        double moveRatio = (double) movedCount / totalPartitions;
        if (moveRatio < minMoveRatio) {
            return false;
        }
        return nowMs - lastRunMs >= cooldownMs;
    }
}
