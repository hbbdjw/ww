package com.ww.runtime.capability;

/**
 * 分区隔离（DEGRADED）视图端口：数据面（网关）在路由前查询分区是否被隔离。
 *
 * <p>隔离语义见 {@code PartitionDegradedException}：属主死亡且无存活副本的分区，
 * 放置保持指向死亡节点（数据留盘押注重启），数据面拒绝服务而非静默空读/空写。</p>
 *
 * <p>实现为带 TTL 的内存快照（宿主周期性从 {@code DegradedPartitionStore} 刷新），
 * 保证数据面热路径无 DB 访问；隔离集合通常为空，查询退化为一次 map 查找。</p>
 */
public interface DegradedPartitionView {

    /** 指定能力的分区是否处于隔离状态（未隔离/无数据返回 false）。 */
    boolean isDegraded(String capability, int partition);
}
