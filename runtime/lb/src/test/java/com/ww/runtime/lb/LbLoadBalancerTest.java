package com.ww.runtime.lb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LbLoadBalancerTest {

    private static LbNodeView node(String id, int httpPort, double weight, boolean active) {
        return new LbNodeView(id, "host-" + id, httpPort, weight, active);
    }

    private static LbNodeView active(String id, int port, double weight) {
        return node(id, port, weight, true);
    }

    private static LbLoadBalancer balancer(List<LbNodeView> nodes, LbConfig config) {
        LbLoadBalancer lb = new LbLoadBalancer(() -> nodes);
        lb.updateConfig(config);
        return lb;
    }

    private static LbConfig enabled(LbStrategy strategy, boolean selfServe, Map<String, Double> overrides) {
        return new LbConfig(true, "node-1", strategy, selfServe,
                List.of("/cache/**", "/storage/**"), overrides,
                3000, 1000, 3000, 3_600_000L, 0);
    }

    private static Map<String, Long> pickDistribution(LbLoadBalancer lb, String self, int times) {
        Map<String, Long> counts = new java.util.TreeMap<>();
        for (int i = 0; i < times; i++) {
            LbNodeView pick = lb.select(self).orElseThrow();
            counts.merge(pick.nodeId(), 1L, Long::sum);
        }
        return counts;
    }

    @Test
    void roundRobinCyclesInStableOrder() {
        List<LbNodeView> nodes = List.of(
                active("node-3", 8093, 1),
                active("node-1", 8091, 1),
                active("node-2", 8092, 1));
        LbLoadBalancer lb = balancer(nodes, enabled(LbStrategy.ROUND_ROBIN, true, Map.of()));

        List<String> picks = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            picks.add(lb.select("node-1").orElseThrow().nodeId());
        }
        assertThat(picks).containsExactly(
                "node-1", "node-2", "node-3", "node-1", "node-2", "node-3", "node-1");
    }

    @Test
    void weightedDistributionMatchesRatioOneTwoThree() {
        List<LbNodeView> nodes = List.of(
                active("node-1", 8091, 1),
                active("node-2", 8092, 2),
                active("node-3", 8093, 3));
        LbLoadBalancer lb = balancer(nodes, enabled(LbStrategy.WEIGHTED, true, Map.of()));

        Map<String, Long> dist = pickDistribution(lb, "node-1", 600);
        assertThat(dist.get("node-1")).isBetween(80L, 120L);
        assertThat(dist.get("node-2")).isBetween(180L, 220L);
        assertThat(dist.get("node-3")).isBetween(280L, 320L);
    }

    @Test
    void weightedIsSmoothLowWeightNodeEvenlySpaced() {
        // 两节点 1:9 极端比例：SWRR 的平滑性保证是低权重节点等间隔出现（每 10 次 1 个），
        // 而非"不连续"（该比例下高权重节点数学上必然出现连续段）。
        List<LbNodeView> nodes = List.of(
                active("node-1", 8091, 1),
                active("node-2", 8092, 9));
        LbLoadBalancer lb = balancer(nodes, enabled(LbStrategy.WEIGHTED, true, Map.of()));

        int totalWeight = 10;
        int rounds = 200;
        StringBuilder seq = new StringBuilder(rounds);
        for (int i = 0; i < rounds; i++) {
            String id = lb.select("node-1").orElseThrow().nodeId();
            seq.append(id.equals("node-1") ? '1' : '2');
        }
        // 每个 totalWeight 大小的滑动窗口中低权重节点恰好出现 1 次（等间隔、无聚集）
        for (int start = 0; start + totalWeight <= rounds; start++) {
            int count = 0;
            for (int i = start; i < start + totalWeight; i++) {
                if (seq.charAt(i) == '1') {
                    count++;
                }
            }
            assertThat(count)
                    .as("window [%d,%d) must contain exactly 1 low-weight pick", start, start + totalWeight)
                    .isEqualTo(1);
        }
    }

    @Test
    void weightOverrideTakesPrecedence() {
        List<LbNodeView> nodes = List.of(
                active("node-1", 8091, 1),
                active("node-2", 8092, 1));
        // node-2 注册权重 1，LB 覆盖为 3 → 约占 3/4
        LbLoadBalancer lb = balancer(nodes,
                enabled(LbStrategy.WEIGHTED, true, Map.of("node-2", 3.0)));
        Map<String, Long> dist = pickDistribution(lb, "node-1", 400);
        assertThat(dist.get("node-2")).isBetween(270L, 330L);
    }

    @Test
    void randomHitsEveryHealthyCandidate() {
        List<LbNodeView> nodes = List.of(
                active("node-1", 8091, 1),
                active("node-2", 8092, 1),
                active("node-3", 8093, 1));
        LbLoadBalancer lb = balancer(nodes, enabled(LbStrategy.RANDOM, true, Map.of()));
        Map<String, Long> dist = pickDistribution(lb, "node-1", 600);
        assertThat(dist).hasSize(3);
        // 等概率：每节点约 200，宽松断言
        dist.values().forEach(v -> assertThat(v).isBetween(140L, 260L));
    }

    @Test
    void inactiveAndUnknownHttpPortNodesAreExcluded() {
        List<LbNodeView> nodes = List.of(
                active("node-1", 8091, 1),
                node("node-2", 8092, 1, false),   // SUSPECTED/DEAD/JOINING
                active("node-3", 0, 1));          // http 端口未上报
        LbLoadBalancer lb = balancer(nodes, enabled(LbStrategy.ROUND_ROBIN, true, Map.of()));
        for (int i = 0; i < 20; i++) {
            assertThat(lb.select("node-1").orElseThrow().nodeId()).isEqualTo("node-1");
        }
    }

    @Test
    void selfServeFalseExcludesEntryNode() {
        List<LbNodeView> nodes = List.of(
                active("node-1", 8091, 1),
                active("node-2", 8092, 1),
                active("node-3", 8093, 1));
        LbLoadBalancer lb = balancer(nodes, enabled(LbStrategy.ROUND_ROBIN, false, Map.of()));
        for (int i = 0; i < 30; i++) {
            String id = lb.select("node-1").orElseThrow().nodeId();
            assertThat(id).isNotEqualTo("node-1");
        }
    }

    @Test
    void retryCanExcludeFailedNode() {
        List<LbNodeView> nodes = List.of(
                active("node-1", 8091, 1),
                active("node-2", 8092, 1),
                active("node-3", 8093, 1));
        LbLoadBalancer lb = balancer(nodes, enabled(LbStrategy.ROUND_ROBIN, true, Map.of()));
        for (int i = 0; i < 30; i++) {
            Optional<LbNodeView> pick = lb.select("node-1", "node-2");
            assertThat(pick.orElseThrow().nodeId()).isNotEqualTo("node-2");
        }
    }

    @Test
    void noHealthyCandidateReturnsEmpty() {
        List<LbNodeView> nodes = List.of(node("node-2", 8092, 1, false));
        LbLoadBalancer lb = balancer(nodes, enabled(LbStrategy.ROUND_ROBIN, true, Map.of()));
        assertThat(lb.select("node-1")).isEmpty();

        // selfServe=false 且候选中只有入口自身
        List<LbNodeView> onlyEntry = List.of(active("node-1", 8091, 1));
        LbLoadBalancer lb2 = balancer(onlyEntry, enabled(LbStrategy.ROUND_ROBIN, false, Map.of()));
        assertThat(lb2.select("node-1")).isEmpty();
    }

    @Test
    void distributionAcrossStrategiesUsesDirectorySnapshot() {
        // 目录快照动态变化：node-2 失活后立即不再被选中
        List<LbNodeView> mutable = new ArrayList<>(List.of(
                active("node-1", 8091, 1),
                active("node-2", 8092, 1)));
        LbLoadBalancer lb = balancer(mutable, enabled(LbStrategy.ROUND_ROBIN, true, Map.of()));
        Map<String, Long> before = pickDistribution(lb, "node-1", 100);
        assertThat(before).containsOnlyKeys("node-1", "node-2");
        mutable.clear();
        mutable.add(active("node-1", 8091, 1));
        Map<String, Long> after = pickDistribution(lb, "node-1", 100);
        assertThat(after).containsOnlyKeys("node-1");
    }

    @Test
    void baseUrlRenderedFromHostAndPort() {
        assertThat(active("node-2", 8092, 1).baseUrl()).isEqualTo("http://host-node-2:8092");
        LbNodeView https = new LbNodeView("node-9", "https://lb.example.com", 443, 1, true);
        assertThat(https.baseUrl()).isEqualTo("https://lb.example.com:443");
    }
}
