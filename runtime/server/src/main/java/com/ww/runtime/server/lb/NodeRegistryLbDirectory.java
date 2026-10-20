package com.ww.runtime.server.lb;

import java.util.List;
import java.util.Objects;

import com.ww.runtime.cluster.NodeRegistry;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.lb.LbNodeDirectory;
import com.ww.runtime.lb.LbNodeView;

/**
 * 集群注册表 → {@link LbNodeDirectory} 适配，带本地快照缓存。
 *
 * <p>{@link #refresh()} 由定时任务周期调用（读共享注册表），代理热路径只读 volatile
 * 快照，不直接查库。仅 {@link NodeState#ACTIVE} 节点进入候选（JOINING/SUSPECTED/
 * DEAD/LEAVING 一律剔除），健康判定完全来自现有心跳状态机，无任何主动探测。</p>
 */
public final class NodeRegistryLbDirectory implements LbNodeDirectory {

    private final NodeRegistry registry;
    private volatile List<LbNodeView> cached = List.of();

    public NodeRegistryLbDirectory(NodeRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @Override
    public List<LbNodeView> snapshot() {
        return cached;
    }

    /** 从注册表重新拉取 ACTIVE 节点视图（httpPort<=0 的节点保留在视图中，由选择器过滤）。 */
    public void refresh() {
        cached = registry.listAlive().stream()
                .filter(info -> info.state() == NodeState.ACTIVE)
                .map(info -> new LbNodeView(
                        info.id().value(), info.host(), info.httpPort(), info.weight(), true))
                .toList();
    }
}
