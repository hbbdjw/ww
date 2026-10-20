package com.ww.runtime.cluster;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.NodeInfo;
import com.ww.runtime.common.NodeState;
import com.ww.runtime.common.ResourceSnapshot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * 进程内 {@link NodeRegistry} 实现：{@link ConcurrentHashMap} 存储，天然线程安全。
 *
 * <p>心跳时刻由 {@code ResourceSnapshot.updatedAtMillis()} 承载（NodeInfo 无独立心跳字段），
 * 与 JDBC 实现中 heartbeat_at/resources_at 列的语义一致。</p>
 */
public final class InMemoryNodeRegistry implements NodeRegistry {

    private final ConcurrentHashMap<NodeId, NodeInfo> nodes = new ConcurrentHashMap<>();

    @Override
    public void register(NodeInfo info) {
        Objects.requireNonNull(info, "info");
        NodeInfo joining = NodeInfo.of(info.id(), info.host(), info.grpcPort(), info.httpPort(),
                NodeState.JOINING, 0L, info.weight(), info.resources(), info.zone());
        nodes.put(info.id(), joining);
    }

    @Override
    public void heartbeat(NodeId id, ResourceSnapshot snapshot) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(snapshot, "snapshot");
        compute(id, current -> current.withState(current.state(), current.stateEpoch(), snapshot));
    }

    @Override
    public void markState(NodeId id, NodeState newState) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(newState, "newState");
        compute(id, current -> {
            NodeStateMachine.check(current.state(), newState);
            return current.withState(newState, current.stateEpoch() + 1, current.resources());
        });
    }

    @Override
    public Optional<NodeInfo> get(NodeId id) {
        Objects.requireNonNull(id, "id");
        return Optional.ofNullable(nodes.get(id));
    }

    @Override
    public List<NodeInfo> listAlive() {
        return nodes.values().stream()
                .filter(info -> info.state() != NodeState.DEAD && info.state() != NodeState.LEAVING)
                .toList();
    }

    @Override
    public List<NodeInfo> listAll() {
        return List.copyOf(nodes.values());
    }

    @Override
    public void tombstone(NodeId id) {
        Objects.requireNonNull(id, "id");
        nodes.compute(id, (key, current) -> {
            if (current == null || current.state() == NodeState.DEAD) {
                return current;
            }
            return current.withState(NodeState.DEAD, current.stateEpoch() + 1,
                    current.resources());
        });
    }

    /**
     * 在 ConcurrentHashMap.compute 的原子语义内完成「存在性检查 + 派生替换」：
     * 节点不存在抛 IllegalArgumentException、非法跃迁抛 IllegalStateTransitionException，
     * 两种情况下映射均保持不变。
     */
    private void compute(NodeId id, UnaryOperator<NodeInfo> mapper) {
        nodes.compute(id, (key, current) -> {
            if (current == null) {
                throw unknownNode(id);
            }
            return mapper.apply(current);
        });
    }

    private static IllegalArgumentException unknownNode(NodeId id) {
        return new IllegalArgumentException("unknown node: " + id);
    }
}
