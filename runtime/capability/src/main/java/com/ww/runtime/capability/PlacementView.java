package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.StaleEpochException;

import java.util.List;
import java.util.Optional;

/**
 * 放置视图端口（防依赖倒置）：能力模块只依赖本接口获取"某能力某分区当前归谁"，
 * 而不直接依赖 placement 模块；宿主在组装时用 placement 的实际实现适配本端口。
 *
 * <p>典型用途：fencing 校验。能力模块在写入前调用 {@link #owner}，
 * 校验（1）该分区已有 owner；（2）owner 是本节点；（3）纪元与请求一致，
 * 任一不满足即拒绝写入（参见 {@link MockCapability}）。
 *
 * <p>适配示例（宿主侧）：
 * <pre>{@code
 * PlacementView view = (cap, partition) -> placementEngine
 *         .lookupOwner(cap, partition)
 *         .map(a -> new PlacementView.Owner(a.nodeId(), a.epoch()));
 * }</pre>
 */
public interface PlacementView {

    /**
     * 查询指定能力在指定分区上的当前属主（PRIMARY）。
     *
     * @param cap       能力类型
     * @param partition 分区
     * @return 属主（含纪元）；分区尚无属主时返回 {@link Optional#empty()}
     */
    Optional<Owner> owner(CapabilityType cap, PartitionId partition);

    /**
     * 查询指定能力在指定分区上的全部持有者（v2 副本视图）。
     *
     * <p>典型用途：写后复制——主写成功后取 REPLICA 持有者并行复制。
     *
     * @return [0]=PRIMARY（含纪元），其余 REPLICA（纪元与主行对齐）；
     *         分区无放置时返回空表
     */
    default List<Owner> holders(CapabilityType cap, PartitionId partition) {
        // 默认实现退化为仅主持有者（单副本视角），宿主用多行放置存储覆写
        return owner(cap, partition).map(List::of).orElse(List.of());
    }

    /**
     * 该能力全部已加载分区的持有者视图（partition → [PRIMARY, REPLICA...]）。
     *
     * <p>反熵/副本修复需要遍历「本机为主但当前无对象」的分区（整分区删除后的
     * 僵尸副本清理），仅靠对象枚举无法覆盖。默认返回空表（不支持全量枚举）。</p>
     */
    default java.util.Map<Integer, List<Owner>> holdersAll(CapabilityType cap) {
        return java.util.Map.of();
    }

    /**
     * 路由视图是否已完成该能力的首次加载。
     *
     * <p>本地缓存实现必须区分“视图未就绪”与“该能力确实没有放置行”：前者应
     * fail-closed（拒绝服务/回退数据库），不能按空放置退化为本地处理。</p>
     */
    default boolean isReady(CapabilityType cap) {
        return true;
    }

    /**
     * 强制刷新路由视图（stale-epoch 重试前调用）。
     *
     * <p>同步存储视图无需刷新；本地缓存视图在此刻重读修订号，避免“重取属主”仍命中
     * 同一陈旧条目。默认 no-op。</p>
     */
    default void refreshRoutingView() {
    }

    /**
     * 写归属校验（fencing）：无属主、属主非本节点或纪元不符时拒绝写入。
     *
     * <p>默认实现面向同步存储视图（每次读取最新 placement）；本地缓存视图可覆写
     * 该方法，在同样的 owner/epoch 校验之外增加 owner 生效时间与本地租约校验，
     * 使写路径不再同步查询数据库。</p>
     */
    default void checkWritable(CapabilityType cap, PartitionId partition, NodeId self,
                               long expectedEpoch) {
        Optional<Owner> owner = owner(cap, partition);
        if (owner.isEmpty()) {
            throw new StaleEpochException(expectedEpoch, -1L);
        }
        Owner current = owner.get();
        if (!current.node().equals(self)
                || (expectedEpoch >= 0 && current.epoch() != expectedEpoch)) {
            throw new StaleEpochException(expectedEpoch, current.epoch());
        }
    }

    /**
     * 分区属主：节点 + 纪元 + owner 最早可写时间。
     *
     * @param node         属主节点
     * @param epoch        属主生效纪元（单调递增，用于 fencing 旧写）
     * @param validAfterMs 新属主最早可接受写入的时间（毫秒时间戳）；0 表示立即生效。
     *                     切主时用于等待旧属主租约过期，避免新旧属主重叠写入。
     */
    record Owner(NodeId node, long epoch, long validAfterMs) {

        public Owner(NodeId node, long epoch) {
            this(node, epoch, 0L);
        }

        public Owner {
            if (node == null) {
                throw new IllegalArgumentException("Owner.node must not be null");
            }
            if (epoch < 0) {
                throw new IllegalArgumentException("Owner.epoch must be >= 0, got " + epoch);
            }
            if (validAfterMs < 0) {
                throw new IllegalArgumentException(
                        "Owner.validAfterMs must be >= 0, got " + validAfterMs);
            }
        }
    }
}
