package com.ww.storage;

import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 存储节点间通信客户端接口：抽象节点间内部通信操作。
 *
 * <p>设计要点：
 * <ul>
 *   <li>与具体传输协议解耦（HTTP/gRPC 均可实现）</li>
 *   <li>大对象读写采用流式 {@link InputStream}，避免全量驻留内存</li>
 *   <li>元数据通过请求/响应头传递（与 HTTP 头语义一致）</li>
 * </ul>
 *
 * <p>提供两种实现：
 * <ul>
 *   <li>{@link StorageNodeClient} — 基于 JDK HttpClient 的 HTTP/1.1 实现（当前默认）</li>
 *   <li>{@code GrpcStorageRpcClient} — 基于 cluster-rpc gRPC 的实现（阶段3新增）</li>
 * </ul>
 */
public interface StorageRpcClient {

    // ---------------- 转发写（到主） ----------------

    /** 转发写请求到主节点，返回主节点计算后的元数据（ETag/version 等） */
    ObjectMetadata forwardPut(StorageNode primary, String bucket, String key,
                              Supplier<InputStream> body, long length,
                              String contentType, Map<String, String> userMeta);

    // ---------------- 复制（主→副本） ----------------

    /** 将主节点本地对象数据复制到副本节点（流式） */
    void replicatePut(StorageNode replica, String bucket, String key,
                      Supplier<InputStream> body, long length, ObjectMetadata meta);

    /** 通知副本删除对象 */
    void replicateDelete(StorageNode replica, String bucket, String key);

    // ---------------- 转发读 ----------------

    /** 转发读请求到主节点，返回流式句柄（调用方必须 close） */
    ReadHandle forwardGet(StorageNode primary, String bucket, String key);

    /** 转发 head（仅元数据） */
    ObjectMetadata forwardHead(StorageNode primary, String bucket, String key);

    /** 转发删除（主节点删除并复制到其副本） */
    void forwardDelete(StorageNode primary, String bucket, String key);

    // ---------------- Bucket 操作 ----------------

    /** 通知对端节点创建 Bucket（本地写） */
    void forwardCreateBucket(StorageNode node, String name);

    /** 通知对端节点删除 Bucket（本地删） */
    void forwardDeleteBucket(StorageNode node, String name, boolean recursive);

    /** 从对端节点拉取 Bucket 内全部对象元数据（用于分布式 list 合并） */
    ObjectMetadata[] forwardListObjects(StorageNode node, String bucket);

    // ---------------- 拓扑与维护态广播 ----------------

    /**
     * 向对端节点广播新拓扑。
     * @param node 目标节点
     * @param topologyPayload JSON 编码的节点列表
     * @param replicas 副本数
     * @return 是否成功（2xx）
     */
    boolean broadcastTopology(StorageNode node, String topologyPayload, int replicas);

    /** 向对端广播维护态切换 */
    boolean broadcastMaintenance(StorageNode node, boolean enabled);

    /**
     * 通知对端节点本地删除对象（不触发复制）：用于迁移完成后清理源端数据。
     */
    boolean localDelete(StorageNode node, String bucket, String key);

    // ---------------- failover 心跳与事件广播 ----------------

    /**
     * 心跳探测对端节点是否存活。
     * @return true=存活（2xx），false=不可达/超时
     */
    boolean ping(StorageNode node, long timeoutMs);

    /**
     * 向对端广播 failover 事件：对端更新本地 nodeStatus + failoverEpoch + 重建路由。
     */
    boolean broadcastFailover(StorageNode node, String failedNodeKey, String statusName, long epoch);

    /**
     * 触发原主节点执行反向同步（DEAD 恢复后从新 primary 拉取落后数据）。
     */
    boolean triggerReverseReconcile(StorageNode node);
}
