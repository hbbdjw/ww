package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PlacementRole;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
 *
 * <p><b>S3（计算成本随规模膨胀的止血）</b>：结果缓存——键为节点集指纹
 * （排序后 {@code (nodeId,weight)} 串的 MD5 前 8 字节，MD5 族与放置哈希同族、
 * 跨进程可复现）+ partitionCount + rf；命中直接返回（{@code List} 已不可变共享，
 * 全量结果 O(P×rf) 堆内存换 O(P×N) MD5 的消失）。调谐每周期对同一节点集
 * 多次 compute（drain 判定、topUp、治愈）从数十秒级降到微秒级。
 * 缓存容量按 LRU 限 32 条（能力类型数量级 ×2），结果不可变故可安全共享。</p>
 */
public final class PlacementEngine {

    /** S3：结果缓存容量（LRU；放置结果不可变，共享安全）。 */
    private static final int COMPUTE_CACHE_MAX = 32;

    /** S3：LRU 缓存（engine 实例可能被多 leader 周期复用，加锁保护）。 */
    private final LinkedHashMap<String, Map<Integer, List<Placement>>> computeCache =
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Map<Integer, List<Placement>>> eldest) {
                    return size() > COMPUTE_CACHE_MAX;
                }
            };

    /**
     * 全量计算每个分区的放置列表（S3：结果缓存，同一节点集指纹直接命中）。
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
        String cacheKey = cacheKey(nodes, partitionCount, replicationFactor);
        if (cacheKey != null) {
            synchronized (computeCache) {
                Map<Integer, List<Placement>> cached = computeCache.get(cacheKey);
                if (cached != null) {
                    return cached;
                }
            }
        }
        Map<Integer, List<Placement>> calculated =
                Collections.unmodifiableMap(calculate(nodes, partitionCount, replicationFactor));
        if (cacheKey != null) {
            synchronized (computeCache) {
                computeCache.put(cacheKey, calculated);
            }
        }
        return calculated;
    }

    /**
     * 只计算每个分区的主节点（replicationFactor=1 简化版；同走 S3 缓存）。
     *
     * @return partition → primary 节点（节点为空 → 空 map）
     */
    public Map<Integer, NodeId> computePrimaries(List<PlacementNode> nodes, int partitionCount) {
        Map<Integer, List<Placement>> computed = compute("primaries", nodes, partitionCount, 1);
        Map<Integer, NodeId> primaries = new LinkedHashMap<>();
        computed.forEach((partition, placements) -> primaries.put(partition, placements.getFirst().node()));
        return Collections.unmodifiableMap(primaries);
    }

    /**
     * S3 缓存键：节点集指纹 = 排序后 (nodeId,weight) 拼接串的 MD5 前 8 字节 hex。
     * 节点列表必须已按 id 排序（ReconcileLoop 的 placementNodes 构造保证）；
     * 空节点集不缓存（路径本就零成本）。
     */
    private static String cacheKey(List<PlacementNode> nodes, int partitionCount, int rf) {
        if (nodes.isEmpty()) {
            return null;
        }
        StringBuilder fingerprint = new StringBuilder(nodes.size() * 32);
        for (PlacementNode node : nodes) {
            fingerprint.append(node.id().value()).append(':')
                    .append(Double.toString(node.weight())).append(':')
                    .append(node.zone()).append(',');
        }
        long hash = md5Prefix8(fingerprint.toString());
        return Long.toHexString(hash) + '#' + partitionCount + '#' + rf;
    }

    /** MD5 前 8 字节（无符号）——与 {@link WeightedRendezvousHash} 同族（MD5 族，跨进程可复现）。 */
    private static long md5Prefix8(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5")
                    .digest(input.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available", e);
        }
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
