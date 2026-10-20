package com.ww.runtime.data.storage;

import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Optional;

/**
 * 存储对等节点目录端口（防依赖倒置）：data 模块经本端口解析「节点 → gRPC 数据面地址」，
 * 不直接依赖 cluster 模块；宿主（runtime-server）用 NodeRegistry 适配。
 *
 * <p>典型用途：
 * <ul>
 *   <li>{@link StorageGrpcClient}：按节点解析 host:grpcPort 建立/缓存通道；</li>
 *   <li>网关 fan-out（列举/桶广播）：{@link #servingPeers()} 提供正在服务本能力的存活节点。</li>
 * </ul>
 */
public interface StoragePeerDirectory {

    /**
     * 解析节点的 gRPC 数据面端点。
     *
     * @return 节点未知或无可用端口时返回 {@code empty()}
     */
    Optional<PeerEndpoint> endpoint(NodeId node);

    /** 当前存活节点全集（含 self；本地快路径由调用方自行短路）。 */
    List<NodeId> alivePeers();

    /**
     * 当前<b>正在提供本能力服务</b>的存活节点（含 self；本地快路径由调用方自行短路）。
     *
     * <p>fan-out（列举/桶广播）应使用本方法而非 {@link #alivePeers()}：集群存活但
     * 能力已停用（如节点被禁用、能力 actual=DISABLED）的节点无法服务数据面请求，
     * 向其转发只会得到「capability not started」拒绝。生产适配器按能力实际态过滤；
     * 默认退化为 {@link #alivePeers()}（单测桩/单机模式无能力状态可查）。</p>
     */
    default List<NodeId> servingPeers() {
        return alivePeers();
    }

    /** gRPC 数据面端点。 */
    record PeerEndpoint(String host, int grpcPort) {

        public PeerEndpoint {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            if (grpcPort <= 0) {
                throw new IllegalArgumentException("grpcPort must be > 0, got " + grpcPort);
            }
        }

        public String target() {
            return host + ":" + grpcPort;
        }
    }
}
