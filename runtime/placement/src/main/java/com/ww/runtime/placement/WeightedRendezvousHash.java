package com.ww.runtime.placement;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 泛型加权 Rendezvous Hash（HRW）——最高随机权重哈希。
 *
 * <p>哈希数学：h = unsigned(MD5(input + ":" + nodeKey) 前 8 字节) / Long.MAX_VALUE。
 * 节点抽象为 {@link WeightedNode}，与注册表实现解耦。</p>
 *
 * <p>对同一 input，各节点分数相互独立且确定，按分数降序取前 N 个节点即为
 * PRIMARY + 副本序；权重比例近似等于分到的 input 比例（加权 HRW 性质）。</p>
 */
public final class WeightedRendezvousHash<N extends WeightedNode> {

    /** MD5 MessageDigest 对象池：虚拟线程下 ThreadLocal 会为每个虚拟线程创建独立实例，
     *  改用共享池避免实例爆炸。 */
    private static final Deque<MessageDigest> MD5_POOL = new ConcurrentLinkedDeque<>();

    /** 池上限，防止无限增长。 */
    private static final int MD5_POOL_MAX = 64;

    private final List<N> nodes;
    /**
     * S3 微优化：构造期一次性把 nodeKey 预编码为 UTF-8 字节，
     * 消除 {@code computeWeightedScore} 每 (input,node) 的字符串拼接与重复编码
     * （10k 分区 × N 节点 → 每节点仅编码一次）。
     */
    private final byte[][] keyBytes;

    public WeightedRendezvousHash(List<N> nodes) {
        Objects.requireNonNull(nodes, "nodes");
        this.nodes = List.copyOf(nodes);
        this.keyBytes = new byte[this.nodes.size()][];
        for (int i = 0; i < this.nodes.size(); i++) {
            this.keyBytes[i] = this.nodes.get(i).nodeKey().getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * 为指定 input 选出前 {@code count} 个节点（按分数降序）。
     *
     * <p>故障域反亲和：PRIMARY 取最高分节点；REPLICA 优先选择与已选节点不同
     * {@code zone} 的最高分节点，无可用新故障域时回退为全局最高分节点。
     * 所有节点 zone 为空/相同时，选择结果与纯 top-N 完全一致（向后兼容）。</p>
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

        // S3：input 字节本周期一次性编码，逐节点 md.update 复用
        byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
        // 计算每个节点的加权分数
        double[] scores = new double[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            scores[i] = computeWeightedScore(inputBytes, nodes.get(i), keyBytes[i]);
        }

        boolean[] selected = new boolean[nodes.size()];
        java.util.HashSet<String> usedZones = new java.util.HashSet<>();
        List<N> result = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            int bestIdx = -1;
            double bestScore = Double.NEGATIVE_INFINITY;
            int bestNewZoneIdx = -1;
            double bestNewZoneScore = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < nodes.size(); i++) {
                if (selected[i]) {
                    continue;
                }
                if (scores[i] > bestScore) {
                    bestScore = scores[i];
                    bestIdx = i;
                }
                String zone = nodes.get(i).zone();
                if (!zone.isEmpty() && !usedZones.contains(zone) && scores[i] > bestNewZoneScore) {
                    bestNewZoneScore = scores[i];
                    bestNewZoneIdx = i;
                }
            }
            // 首个节点（PRIMARY）不施反亲和；后续副本优先未使用故障域
            int pick = (k > 0 && bestNewZoneIdx >= 0) ? bestNewZoneIdx : bestIdx;
            selected[pick] = true;
            String pickedZone = nodes.get(pick).zone();
            if (!pickedZone.isEmpty()) {
                usedZones.add(pickedZone);
            }
            result.add(nodes.get(pick));
        }
        return result;
    }

    /**
     * 加权分数计算：{@code -ln(h) * weight}，权重越大分数越高，越容易被选中，
     * 节点胜率与权重成正比（权重 (1,2) 的两节点各得约 1/3、2/3 的 key）。
     * 等价于标准加权 HRW 对 {@code -ln(h) / weight} 取最小值，
     * 此处按「分数降序」语义统一为乘权重后取最大值。
     *
     * <p>归一化：哈希值清除符号位后除以 {@code Long.MAX_VALUE}（2^63 - 1）映射到
     * (0, 1)。除数不可用 {@code (1L << 63)}——它在 Java 中等于
     * {@code Long.MIN_VALUE}（负数），会使 h 为负并被钳制为
     * {@code Double.MIN_VALUE}，所有节点分数相同，HRW 退化为「永远选第一个节点」。</p>
     */
    private double computeWeightedScore(byte[] inputBytes, N node, byte[] nodeKeyBytes) {
        long hash = hash(inputBytes, nodeKeyBytes);
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

    private static MessageDigest borrowMd5() {
        MessageDigest md = MD5_POOL.pollFirst();
        if (md != null) {
            return md;
        }
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available", e);
        }
    }

    private static void returnMd5(MessageDigest md) {
        if (MD5_POOL.size() < MD5_POOL_MAX) {
            MD5_POOL.offerFirst(md);
        }
    }

    /**
     * S3：分块 update（inputBytes + ':' + keyBytes）与原 {@code md5(input + ":" + key)}
     * 字节级等价（UTF-8 拼接编码 == 顺序 update），哈希结果不变——
     * 仅消除每 (input,node) 的字符串拼接与重复编码。
     */
    private static long hash(byte[] inputBytes, byte[] nodeKeyBytes) {
        MessageDigest md = borrowMd5();
        try {
            md.reset();
            md.update(inputBytes);
            md.update((byte) ':');
            md.update(nodeKeyBytes);
            byte[] digest = md.digest();
            // 取前 8 字节构造 long
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } finally {
            returnMd5(md);
        }
    }
}
