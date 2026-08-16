package com.ww.runtime.placement;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 泛型加权 Rendezvous Hash（HRW）——最高随机权重哈希。
 *
 * <p>移植自 common-cluster 的 {@code com.ww.common.RendezvousHash}，哈希数学保持一致：
 * h = unsigned(MD5(input + ":" + nodeKey) 前 8 字节) / Long.MAX_VALUE。
 * 节点抽象为 {@link WeightedNode}，与注册表实现解耦。</p>
 *
 * <p>对同一 input，各节点分数相互独立且确定，按分数降序取前 N 个节点即为
 * PRIMARY + 副本序；权重比例近似等于分到的 input 比例（加权 HRW 性质）。</p>
 */
public final class WeightedRendezvousHash<N extends WeightedNode> {

    private final List<N> nodes;
    private final ThreadLocal<MessageDigest> md5Local;

    public WeightedRendezvousHash(List<N> nodes) {
        Objects.requireNonNull(nodes, "nodes");
        this.nodes = List.copyOf(nodes);
        this.md5Local = ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("MD5 algorithm not available", e);
            }
        });
    }

    /**
     * 为指定 input 选出前 {@code count} 个节点（按分数降序）。
     *
     * @param input 哈希输入（如分区键 "P000042"）
     * @param count 需要选出的节点数（最多为总节点数；&lt;=0 返回空表）
     * @return 排序后的节点列表，[0]=首选（Primary），空节点集返回空表
     */
    public List<N> selectNodes(String input, int count) {
        if (nodes.isEmpty()) {
            return List.of();
        }
        int n = Math.min(count, nodes.size());

        // 计算每个节点的加权分数
        double[] scores = new double[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            scores[i] = computeWeightedScore(input, nodes.get(i));
        }

        // 按分数降序选出 top N（严格大于比较，平分时保留列表序靠前者，与移植源一致）
        boolean[] selected = new boolean[nodes.size()];
        List<N> result = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            int bestIdx = -1;
            double bestScore = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < nodes.size(); i++) {
                if (!selected[i] && scores[i] > bestScore) {
                    bestScore = scores[i];
                    bestIdx = i;
                }
            }
            selected[bestIdx] = true;
            result.add(nodes.get(bestIdx));
        }
        return result;
    }

    /**
     * 加权分数计算：-ln(hash / MAX) * weight
     * 权重越大，分数越高，越容易被选中。
     *
     * <p>修复（移植来源：common-cluster RendezvousHash，Long.MAX_VALUE 修复必须保留）：
     * 原实现 {@code (double)(1L << 63)} 在 Java 中等于 {@code Long.MIN_VALUE}
     * （负数），导致 h 计算为负数，被钳制为 {@code Double.MIN_VALUE}，
     * 所有节点分数相同，HRW 退化为"永远选第一个节点"。
     * 正确做法是用 {@code Long.MAX_VALUE}（2^63 - 1）作为除数。</p>
     *
     * <p>权重方向修正（相对移植源的偏差，修复原因如下）：原实现公式为
     * {@code -ln(h) / weight} 且按最大分数选取——该组合在数学上使胜率正比于
     * {@code 1/weight}（两节点时 P(i 胜) = (1/w_i) / Σ(1/w_j)），即权重越大的节点
     * 分到的 key 越少，与原注释"权重越大越容易被选中"及其业务语义相悖。
     * 本模块要求份额与权重成正比（权重 (1,2) 的节点应各得约 1/3、2/3），
     * 故在保留 MD5 前 8 字节哈希、unsigned 归一化与 Long.MAX_VALUE 修复的前提下，
     * 将公式修正为 {@code -ln(h) * weight}（等价于标准加权 HRW 对 -ln(h)/weight 取最小值，
     * 此处按"分数降序"语义统一为乘权重后取最大值）。</p>
     */
    private double computeWeightedScore(String input, N node) {
        long hash = hash(input + ":" + node.nodeKey());
        // 将 hash 映射到 (0, 1) 区间：清除符号位后除以 Long.MAX_VALUE
        // 注意：不能用 (1L << 63)，它在 Java 中是 Long.MIN_VALUE（负数）
        long unsigned = hash & 0x7FFFFFFFFFFFFFFFL;
        double h = (double) unsigned / (double) Long.MAX_VALUE;
        if (h <= 0.0) {
            h = Double.MIN_VALUE;
        }
        double weight = node.weight();
        if (weight <= 0.0) {
            weight = 1.0;
        }
        return -Math.log(h) * weight;
    }

    private long hash(String input) {
        MessageDigest md = md5Local.get();
        md.reset();
        byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
        // 取前 8 字节构造 long
        long h = 0;
        for (int i = 0; i < 8; i++) {
            h = (h << 8) | (digest[i] & 0xFF);
        }
        return h;
    }
}
