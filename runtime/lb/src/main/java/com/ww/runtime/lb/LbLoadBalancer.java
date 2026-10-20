package com.ww.runtime.lb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 负载均衡选节点器（纯 POJO，无 IO）。
 *
 * <p>候选集合 = 目录快照中 {@code active=true && httpPort>0} 的节点；
 * {@code selfServe=false} 时剔除入口节点；重试时可额外剔除指定节点。
 * 候选按 nodeId 稳定排序，保证轮询/权重状态与列表顺序确定。</p>
 *
 * <h3>策略</h3>
 * <ul>
 *   <li>{@link LbStrategy#ROUND_ROBIN}：AtomicLong 游标对候选数取模；</li>
 *   <li>{@link LbStrategy#WEIGHTED}：nginx 平滑加权轮询（有效权重 ×100 取整），
 *       权重 = 页面覆盖 ?? 节点注册权重；候选集合或权重变化时重建内部状态；</li>
 *   <li>{@link LbStrategy#RANDOM}：候选等概率随机（不考虑权重）。</li>
 * </ul>
 */
public final class LbLoadBalancer {

    private final LbNodeDirectory directory;
    private final AtomicLong roundRobinCursor = new AtomicLong();

    private volatile LbConfig config = LbConfig.defaultConfig();

    /** 平滑权重内部状态（仅在 synchronized 块内访问）。 */
    private String weightedSignature = "";
    private long[] weightedCurrent = new long[0];
    private long[] weightedEffective = new long[0];

    public LbLoadBalancer(LbNodeDirectory directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
    }

    /** 热更新配置（由定时刷新任务调用，volatile 发布）。 */
    public void updateConfig(LbConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    /** 按当前策略选择一个后端；无健康候选返回 empty（调用方应答 503）。 */
    public Optional<LbNodeView> select(String selfNodeId) {
        return select(selfNodeId, null);
    }

    /**
     * 选择后端，可排除指定节点（GET/HEAD 连接失败换节点重试时排除失败节点）。
     */
    public Optional<LbNodeView> select(String selfNodeId, String excludeNodeId) {
        LbConfig c = config;
        List<LbNodeView> candidates = candidates(c, excludeNodeId);
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        return switch (c.strategy()) {
            case ROUND_ROBIN -> Optional.of(selectRoundRobin(candidates));
            case WEIGHTED -> Optional.of(selectWeighted(candidates, c));
            case RANDOM -> Optional.of(candidates.get(ThreadLocalRandom.current().nextInt(candidates.size())));
        };
    }

    /** 构建并稳定排序候选列表。 */
    private List<LbNodeView> candidates(LbConfig c, String excludeNodeId) {
        List<LbNodeView> nodes = directory.snapshot();
        List<LbNodeView> candidates = new ArrayList<>(nodes == null ? 0 : nodes.size());
        if (nodes != null) {
            for (LbNodeView node : nodes) {
                if (node == null || !node.active() || node.httpPort() <= 0) {
                    continue;
                }
                // 入口自身不参与：剔除入口节点（过滤器只在本节点==入口时代理，即剔除本机）
                if (!c.selfServe() && node.nodeId().equals(c.entryNodeId())) {
                    continue;
                }
                if (node.nodeId().equals(excludeNodeId)) {
                    continue;
                }
                candidates.add(node);
            }
        }
        candidates.sort(Comparator.comparing(LbNodeView::lowerId));
        return candidates;
    }

    private LbNodeView selectRoundRobin(List<LbNodeView> candidates) {
        long index = Math.floorMod(roundRobinCursor.getAndIncrement(), candidates.size());
        return candidates.get((int) index);
    }

    /** nginx 平滑加权轮询：短临界区保护 currentWeight 状态。 */
    private synchronized LbNodeView selectWeighted(List<LbNodeView> candidates, LbConfig c) {
        int n = candidates.size();
        long[] effective = new long[n];
        StringBuilder signature = new StringBuilder();
        for (int i = 0; i < n; i++) {
            LbNodeView node = candidates.get(i);
            double w = c.effectiveWeight(node.nodeId(), node.weight());
            long scaled = Math.max(1L, Math.round(w * 100.0));
            effective[i] = scaled;
            signature.append(node.nodeId()).append('=').append(scaled).append(';');
        }
        String sig = signature.toString();
        if (!sig.equals(weightedSignature) || weightedCurrent.length != n) {
            weightedSignature = sig;
            weightedEffective = effective;
            weightedCurrent = new long[n];
        }

        long total = 0L;
        int best = -1;
        long bestCurrent = Long.MIN_VALUE;
        for (int i = 0; i < n; i++) {
            weightedCurrent[i] += weightedEffective[i];
            total += weightedEffective[i];
            // 严格大于：权重相等时稳定偏向排序靠前的节点，行为可预测
            if (weightedCurrent[i] > bestCurrent) {
                bestCurrent = weightedCurrent[i];
                best = i;
            }
        }
        weightedCurrent[best] -= total;
        return candidates.get(best);
    }
}
