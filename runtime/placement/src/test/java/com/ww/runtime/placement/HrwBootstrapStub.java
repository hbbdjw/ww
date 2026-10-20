package com.ww.runtime.placement;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 测试内独立实现的加权 HRW 数学基准：
 * 本地 stub 节点（nodeKey + weight），MD5(input + ":" + nodeKey) 前 8 字节构造 64 位值，
 * u = unsigned / Long.MAX_VALUE，score = -ln(u) * weight，最高分者为 primary。
 *
 * <p>刻意不引用生产代码，作为独立基准交叉校验 {@link WeightedRendezvousHash} 的选取结果。</p>
 */
final class HrwBootstrapStub {

    record StubNode(String nodeKey, double weight) {
    }

    private HrwBootstrapStub() {
    }

    /** 与 PlacementEngine 一致的分区输入："P" + %06d(p+1)。 */
    static String partitionInput(int partition) {
        return "P" + String.format(Locale.ROOT, "%06d", partition + 1);
    }

    /** 按顺序 A/B/C 构造 stub 节点集。 */
    static List<StubNode> nodes(double... weights) {
        String[] keys = {"A", "B", "C"};
        List<StubNode> stubs = new ArrayList<>(weights.length);
        for (int i = 0; i < weights.length; i++) {
            stubs.add(new StubNode(keys[i], weights[i]));
        }
        return stubs;
    }

    /** 基准数学：返回最高分节点的 nodeKey。 */
    static String primaryNodeKey(String input, List<StubNode> stubs) throws Exception {
        String bestKey = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (StubNode node : stubs) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest((input + ":" + node.nodeKey()).getBytes(StandardCharsets.UTF_8));
            long h64 = 0;
            for (int i = 0; i < 8; i++) {
                h64 = (h64 << 8) | (digest[i] & 0xFF);
            }
            double u = (double) (h64 & 0x7FFFFFFFFFFFFFFFL) / (double) Long.MAX_VALUE;
            if (u <= 0.0) {
                u = Double.MIN_VALUE;
            }
            double weight = node.weight() <= 0.0 ? 1.0 : node.weight();
            double score = -Math.log(u) * weight;
            if (score > bestScore) {
                bestScore = score;
                bestKey = node.nodeKey();
            }
        }
        return bestKey;
    }
}
