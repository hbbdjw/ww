package com.ww.runtime.placement;

import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 分区隔离注册表：{@code no live replica} 分区在治愈宽限期赌输后的隔离状态存储。
 *
 * <p>设计动机：此前的降级治愈以「空分区迁移」覆盖放置表——把暂时不可用转化为
 * 静默永久丢失（且死亡节点重启后其遗留数据还会被启动 sweep 销毁）。隔离方案
 * 保持放置指向死亡节点（数据留盘），仅标记拒绝数据面流量：</p>
 * <ul>
 *   <li>属主回归 ACTIVE → 调度器自动解除隔离，数据原样恢复（押注重启的正式化）；</li>
 *   <li>属主永不回归 → 运营经 {@code force-adopt} 显式接受丢失（cutover 到存活节点）；</li>
 *   <li>隔离集合通常为空，{@link #partitionsOf} 供调度器每周期一次批量加载。</li>
 * </ul>
 *
 * <p>实现必须线程安全；{@link #mark} 幂等（重复标记刷新 sinceMs/reason 或保持，
 * 由实现决定，调度器依赖 {@link #partitionsOf} 判断「已隔离」跳过重复处理）。</p>
 */
public interface DegradedPartitionStore {

    /** 标记分区隔离（幂等 upsert；已存在时刷新 reason/sinceMs）。 */
    void mark(String capability, int partition, NodeId owner, String reason);

    /** 解除单个分区隔离（不存在时 no-op）。 */
    void clear(String capability, int partition);

    /** 该能力全部隔离分区号（一次批量查询，供调度器/缓存刷新使用）。 */
    Set<Integer> partitionsOf(String capability);

    /** 该能力全部隔离记录（管理面列表）。 */
    List<DegradedPartition> list(String capability);

    /** 全部能力的隔离记录（管理面跨能力列表）。 */
    List<DegradedPartition> listAll();

    /** 单分区隔离状态（数据面/测试用；热路径应走缓存视图）。 */
    boolean isDegraded(String capability, int partition);

    /** 隔离分区 → 属主映射（调度器判定回归解除用）。 */
    Map<Integer, NodeId> ownersOf(String capability);
}
