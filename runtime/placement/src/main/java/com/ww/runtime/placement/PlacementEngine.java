package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * 放置引擎：基于加权 HRW 为每个分区计算 PRIMARY/REPLICA 放置。
 *
 * <p>分区输入固定为 {@code "P" + String.format("%06d", p + 1)}
 * （即 "P000001"、"P000002"、……），因此同一节点集与权重下计算结果恒定，
 * 跨节点/跨重启可复现——这也是 RebalancePlanner「最小移动」性质的基础。</p>
 *
 * <p>epoch 恒为 0（初始全量计算语义）；后续单分区切主由
 * {@link PlacementStore#cutover} 以 CAS 语义推进。</p>
 */
public final class PlacementEngine {

    /**
     * 全量计算每个分区的放置列表。
     *
     * @param capability       放置所属能力（仅作放置集合标识，不参与 hash）
     * @param nodes            候选节点（空 → 空 map）
     * @param partitionCount   分区数
     * @param replicationFactor 每分区副本数（超出节点数时截断为节点数）
     * @return partition → placements（[0]=PRIMARY，其余 REPLICA，epoch=0）
     */
    public Map<Integer, List<Placement>> compute(String capability, List<PlacementNode> nodes,
                                                 int partitionCount, int replicationFactor) {
        Objects.requireNonNull(capability, "capability");
        return Collections.unmodifiableMap(calculate(nodes, partitionCount, replicationFactor));
    }

    /**
     * 只计算每个分区的主节点（replicationFactor=1 简化版）。
     *
     * @return partition → primary 节点（节点为空 → 空 map）
     */
    public Map<Integer, NodeId> computePrimaries(List<PlacementNode> nodes, int partitionCount) {
        Map<Integer, List<Placement>> computed = calculate(nodes, partitionCount, 1);
        Map<Integer, NodeId> primaries = new LinkedHashMap<>();
        computed.forEach((partition, placements) -> primaries.put(partition, placements.getFirst().node()));
        return Collections.unmodifiableMap(primaries);
    }

    private static Map<Integer, List<Placement>> calculate(List<PlacementNode> nodes,
                                                           int partitionCount, int replicationFactor) {
        Objects.requireNonNull(nodes, "nodes");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0, got " + partitionCount);
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be > 0, got " + replicationFactor);
        }
        if (nodes.isEmpty()) {
            return new LinkedHashMap<>();
        }
        WeightedRendezvousHash<PlacementNode> hrw = new WeightedRendezvousHash<>(nodes);
        Map<Integer, List<Placement>> result = new LinkedHashMap<>();
        for (int p = 0; p < partitionCount; p++) {
            String input = "P" + String.format(Locale.ROOT, "%06d", p + 1);
            List<PlacementNode> selected = hrw.selectNodes(input, replicationFactor);
            List<Placement> placements = new ArrayList<>(selected.size());
            for (int i = 0; i < selected.size(); i++) {
                PlacementRole role = i == 0 ? PlacementRole.PRIMARY : PlacementRole.REPLICA;
                placements.add(new Placement(p, selected.get(i).id(), role, 0L));
            }
            result.put(p, List.copyOf(placements));
        }
        return result;
    }
}
