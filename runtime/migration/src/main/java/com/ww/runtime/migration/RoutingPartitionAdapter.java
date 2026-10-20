package com.ww.runtime.migration;

import com.ww.runtime.common.NodeId;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;
import java.util.function.Function;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 节点感知路由适配器：按迁移任务的 <b>source</b>/<b>target</b> 节点把数据面调用路由到
 * 「本地组合适配器 | 远端节点的 {@link RemotePartitionAdapter}」——迁移引擎注入本适配器后
 * 即获得跨节点数据搬迁能力（执行节点 = 源与目标之外的第三方亦可，中继转发）。
 *
 * <p><b>能力无关</b>：capability 参数透传给本地组合适配器按能力分发，任何能力
 * （MOCK/CACHE/STORAGE/…）注册进组合适配器即自动获得跨节点迁移，无需本类感知。</p>
 *
 * <h2>路由规则</h2>
 * <ul>
 *   <li>source/target 为 {@code null}（无历史归属的新分区）：导出/计数按空处理；</li>
 *   <li>source/target == self：走本地组合适配器（同进程直连）；</li>
 *   <li>其余：经缓存的 gRPC 通道访问远端节点（通道按节点缓存，maxInboundMessageSize=8MB）；</li>
 *   <li><b>死源语义</b>：源节点无法解析（非存活/未注册）→ WARN 后按空分区迁移
 *       （数据丢失窗口，与调度器治愈迁移「no live replica」同语义）；源可解析但 RPC
 *       失败 → 异常上抛，任务 FAILED 走冷却重试。</li>
 * </ul>
 *
 * <p>{@link #close()} 释放全部缓存通道（宿主停机时调用）。</p>
 */
public final class RoutingPartitionAdapter implements PartitionDataAdapter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RoutingPartitionAdapter.class);

    /** 远端节点端点（gRPC 数据面地址）。 */
    public record Endpoint(String host, int port) {
        public Endpoint {
            Objects.requireNonNull(host, "host");
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("invalid grpc port: " + port);
            }
        }
    }

    private final PartitionDataAdapter local;
    private final NodeId self;
    private final Function<NodeId, Optional<Endpoint>> resolver;
    private final ConcurrentHashMap<NodeId, ManagedChannel> channels = new ConcurrentHashMap<>();

    /**
     * @param local   本地组合适配器（多能力分发；source/target==self 时直连）
     * @param self    本节点标识
     * @param resolver 节点 → gRPC 端点解析（无法解析视为死节点）
     */
    public RoutingPartitionAdapter(PartitionDataAdapter local,
                                   NodeId self,
                                   Function<NodeId, Optional<Endpoint>> resolver) {
        this.local = Objects.requireNonNull(local, "local");
        this.self = Objects.requireNonNull(self, "self");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
    }

    // ------------------------------------------------------------------
    // 本地直连透传（source/target 缺省或 == self 的兜底路径）
    // ------------------------------------------------------------------

    @Override
    public long itemCount(String capability, int partition) {
        return local.itemCount(capability, partition);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint) {
        return local.exportChunks(capability, partition, fromCheckpoint);
    }

    @Override
    public boolean importChunks(String capability, int partition, List<byte[]> chunks) {
        return local.importChunks(capability, partition, chunks);
    }

    @Override
    public long verifyCount(String capability, int partition) {
        return local.verifyCount(capability, partition);
    }

    // ------------------------------------------------------------------
    // 节点感知路由
    // ------------------------------------------------------------------

    @Override
    public long itemCount(String capability, int partition, NodeId source) {
        if (source == null) {
            return 0L; // 无历史归属的新分区
        }
        if (source.equals(self)) {
            return local.itemCount(capability, partition);
        }
        RemotePartitionAdapter remote = remoteOrNull(capability, partition, source, "itemCount");
        return remote == null ? 0L : remote.countPartition(capability, partition);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint,
                                         NodeId source) {
        return exportChunks(capability, partition, fromCheckpoint, null, source);
    }

    @Override
    public Iterator<byte[]> exportChunks(String capability, int partition, long fromCheckpoint,
                                         String cursor, NodeId source) {
        if (source == null) {
            return Collections.emptyIterator(); // 无历史归属的新分区
        }
        if (source.equals(self)) {
            return local.exportChunks(capability, partition, fromCheckpoint, cursor);
        }
        RemotePartitionAdapter remote = remoteOrNull(capability, partition, source, "exportChunks");
        return remote == null
                ? Collections.emptyIterator()
                : remote.exportChunks(capability, partition, fromCheckpoint, cursor);
    }

    @Override
    public String cursorOf(String capability, byte[] piece) {
        // 分片格式由能力适配器定义（与源节点位置无关），本地组合器即可解析
        return local.cursorOf(capability, piece);
    }

    @Override
    public boolean supportsCursorResume() {
        return local.supportsCursorResume();
    }

    @Override
    public boolean freezePartition(String capability, int partition, NodeId source,
                                   long ttlMillis) {
        if (source == null || source.equals(self)) {
            return local.freezePartition(capability, partition, ttlMillis);
        }
        RemotePartitionAdapter remote = remoteOrNull(capability, partition, source, "freezePartition");
        if (remote == null) {
            // 源不可解析（死源/已下线）：按空分区迁移语义，无写入需要停——冻结视为已生效
            LOG.warn("freeze skipped for {}/{}: source {} not resolvable (dead source semantics)",
                    capability, partition, source);
            return true;
        }
        return remote.freezePartition(capability, partition, ttlMillis);
    }

    @Override
    public boolean importChunks(String capability, int partition, List<byte[]> chunks,
                                NodeId target, long taskId) {
        if (target.equals(self)) {
            return local.importChunks(capability, partition, chunks);
        }
        RemotePartitionAdapter remote = remoteOrNull(capability, partition, target, "importChunks");
        if (remote == null) {
            // 目标不可达：无法迁入，任务 FAILED 重试（目标节点可能正在启动/瞬时失联）
            throw new com.ww.runtime.common.MigrationException(
                    "import target unreachable: capability=" + capability
                            + " partition=" + partition + " target=" + target);
        }
        // 任务绑定适配器：接收端 TransferStream 按 taskId 持久化进度。
        // 共享底层缓存通道且本适配器无其他资源——不 close（通道生命周期由本类管理）。
        return new RemotePartitionAdapter(channelFor(target), taskId)
                .importChunks(capability, partition, chunks);
    }

    @Override
    public boolean importChunks(String capability, int partition, List<byte[]> chunks,
                                NodeId target, long taskId, List<String> checksums) {
        if (target.equals(self)) {
            return local.importChunks(capability, partition, chunks);
        }
        RemotePartitionAdapter remote = remoteOrNull(capability, partition, target, "importChunks");
        if (remote == null) {
            throw new com.ww.runtime.common.MigrationException(
                    "import target unreachable: capability=" + capability
                            + " partition=" + partition + " target=" + target);
        }
        // 携带引擎预计算校验和透传，传输层不再逐片重复哈希
        return new RemotePartitionAdapter(channelFor(target), taskId)
                .importChunks(capability, partition, chunks, target, taskId, checksums);
    }

    @Override
    public long verifyCount(String capability, int partition, NodeId target) {
        if (target.equals(self)) {
            return local.verifyCount(capability, partition);
        }
        RemotePartitionAdapter remote = remoteOrNull(capability, partition, target, "verifyCount");
        return remote == null ? 0L : remote.countPartition(capability, partition);
    }

    @Override
    public boolean clearPartition(String capability, int partition, NodeId source) {
        if (source == null || source.equals(self)) {
            return local.clearPartition(capability, partition, source);
        }
        RemotePartitionAdapter remote = remoteOrNull(capability, partition, source, "clearPartition");
        return remote != null && remote.clearPartition(capability, partition);
    }

    // ------------------------------------------------------------------
    // 本地清扫/压实透传（启动清扫器作用于本节点数据）
    // ------------------------------------------------------------------

    @Override
    public long retainPartitions(String capability, IntPredicate keep) {
        return local.retainPartitions(capability, keep);
    }

    @Override
    public void compact(String capability) {
        local.compact(capability);
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /**
     * 解析并连接远端节点；无法解析（死节点）→ WARN + null（调用方按空分区/不可达语义处理）。
     */
    private RemotePartitionAdapter remoteOrNull(String capability, int partition,
                                                NodeId node, String op) {
        Optional<Endpoint> endpoint = resolver.apply(node);
        if (endpoint.isEmpty()) {
            LOG.warn("migration {} for {}/{}: source/target node {} not resolvable "
                    + "(dead or not ACTIVE); treating as empty partition (data loss window if source)",
                    op, capability, partition, node);
            return null;
        }
        return new RemotePartitionAdapter(channelFor(node));
    }

    /** 按节点缓存 gRPC 通道（plaintext 内网，8MB 消息上限与 RemotePartitionAdapter 同约定）。 */
    private ManagedChannel channelFor(NodeId node) {
        Endpoint endpoint = resolver.apply(node)
                .orElseThrow(() -> new com.ww.runtime.common.MigrationException(
                        "node not resolvable: " + node));
        return channels.computeIfAbsent(node, n -> ManagedChannelBuilder
                .forAddress(endpoint.host(), endpoint.port())
                .maxInboundMessageSize(8 * 1024 * 1024)
                .usePlaintext()
                .build());
    }

    @Override
    public void close() {
        for (Map.Entry<NodeId, ManagedChannel> entry : channels.entrySet()) {
            shutdown(entry.getValue());
        }
        channels.clear();
    }

    private static void shutdown(ManagedChannel channel) {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(3L, java.util.concurrent.TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
