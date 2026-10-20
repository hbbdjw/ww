package com.ww.runtime.data.storage;

import com.ww.runtime.common.NodeId;
import com.ww.runtime.data.storage.engine.ObjectMetadata;

import java.io.InputStream;
import java.util.Optional;

/**
 * 副本复制客户端端口：{@link StorageReplicator} 依赖本接口而非具体 gRPC 实现，
 * 便于以桩实现单测（成功/失败入 backlog/重放对账）。
 *
 * <p>实现约定：方法失败（网络/超时/远端拒绝）返回 {@code false}/{@code empty()} 而不抛出，
 * 由 Replicator 决定入 backlog 重放。
 */
public interface StorageReplicationClient extends AutoCloseable {

    /**
     * 流式复制一个对象到目标节点（Replicate RPC，chunkSize 分片）。
     *
     * @param meta 主节点侧元数据（version/etag 由主预计算，副端不再计算）
     * @return 远端确认落盘返回 true
     */
    boolean replicateObject(NodeId node, String bucket, ObjectMetadata meta, InputStream data);

    /** 副本删除（ReplicateDelete RPC）。 */
    boolean replicateDelete(NodeId node, String bucket, String key);

    /**
     * 带版本护栏的副本删除：副本本地版本高于 {@code version} 时拒绝删除
     * （防止过期删除意图抹掉更新写入）。默认降级为无版本删除（旧实现/测试桩）。
     *
     * @param version 删除意图对应的对象版本；{@code <=0} 表示无版本信息
     */
    default boolean replicateDelete(NodeId node, String bucket, String key, long version) {
        return replicateDelete(node, bucket, key);
    }

    /** 远端 HEAD（对账用）：远端不存在或不可达返回 {@code empty()}。 */
    Optional<ObjectMetadata> remoteHead(NodeId node, String bucket, String key);

    /**
     * T14 对账：远端桶内对象摘要（key+version+etag），按 marker（排他）有界列举一页（≤maxKeys）。
     * 未实现的客户端抛 {@link UnsupportedOperationException}——调用方据此降级逐对象
     * {@link #remoteHead} 路径（返回空列表是合法语义 = 桶为空，不能用作能力探测信号）。
     */
    default java.util.List<ObjectSummary> remoteList(NodeId node, String bucket,
                                                     String marker, int maxKeys) {
        throw new UnsupportedOperationException("object summary listing not supported");
    }

    /** T14 对账用的轻量对象摘要。 */
    record ObjectSummary(String key, long version, String etag) {
    }

    @Override
    void close();
}
