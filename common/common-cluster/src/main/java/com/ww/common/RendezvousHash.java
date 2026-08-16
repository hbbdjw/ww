package com.ww.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Weighted Rendezvous Hash (HRW) — 最高随机权重哈希
 *
 * <p>对每个 key，为每个节点计算加权哈希分数，按分数降序排列，
 * 第一个为 Primary，后续为 Backup。支持节点容量权重。
 */
public class RendezvousHash {

    private final List<ClusterNode> nodes;
    private final ThreadLocal<MessageDigest> md5Local;

    public RendezvousHash(List<ClusterNode> nodes) {
        this.nodes = new ArrayList<>(nodes);
        this.md5Local = ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 为指定 key 选出前 N 个节点（按分数降序）
     *
     * @param key  缓存键
     * @param count 需要选出的节点数（最多为总节点数）
     * @return 排序后的节点列表，[0]=Primary, [1]=Backup1, ...
     */
    public List<ClusterNode> selectNodes(String key, int count) {
        if (nodes.isEmpty()) return List.of();
        int n = Math.min(count, nodes.size());

        // 计算每个节点的加权分数
        double[] scores = new double[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            scores[i] = computeWeightedScore(key, nodes.get(i));
        }

        // 按分数降序选出 top N
        boolean[] selected = new boolean[nodes.size()];
        List<ClusterNode> result = new ArrayList<>(n);
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
     * 为 key 选出首选节点（Primary）
     */
    public ClusterNode selectPrimary(String key) {
        List<ClusterNode> result = selectNodes(key, 1);
        return result.isEmpty() ? null : result.getFirst();
    }

    /**
     * 加权分数计算：-ln(hash / MAX) / weight
     * 权重越大，分数期望越高，越容易被选中
     *
     * <p>修复：原实现 {@code (double)(1L << 63)} 在 Java 中等于 {@code Long.MIN_VALUE}
     * （负数），导致 h 计算为负数，被钳制为 {@code Double.MIN_VALUE}，
     * 所有节点分数相同，HRW 退化为"永远选第一个节点"。
     * 正确做法是用 {@code Long.MAX_VALUE}（2^63 - 1）作为除数。
     */
    private double computeWeightedScore(String key, ClusterNode node) {
        long hash = hash(key + ":" + node.nodeKey());
        // 将 hash 映射到 (0, 1) 区间：清除符号位后除以 Long.MAX_VALUE
        // 注意：不能用 (1L << 63)，它在 Java 中是 Long.MIN_VALUE（负数）
        long unsigned = hash & 0x7FFFFFFFFFFFFFFFL;
        double h = (double) unsigned / (double) Long.MAX_VALUE;
        if (h <= 0.0) h = Double.MIN_VALUE;
        double weight = node.getWeight();
        if (weight <= 0.0) weight = 1.0;
        return -Math.log(h) / weight;
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

    public List<ClusterNode> getNodes() {
        return new ArrayList<>(nodes);
    }
}
