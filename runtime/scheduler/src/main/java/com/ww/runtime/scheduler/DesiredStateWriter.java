package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityStateStore;
import com.ww.runtime.capability.CapabilityStateStore.CapabilityRecord;
import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Objects;

/**
 * 期望态写入器（管理面落地，§60「管理面只写期望态」）：包装
 * {@link CapabilityStateStore#setDesired} 并施加集群级合法性校验。
 *
 * <ul>
 *   <li>desired 只允许 {@code ACTIVE} / {@code DISABLED} 两个开关值——
 *       其余中间态（STARTING/DRAINING/…）由节点状态机自治产出，管理面不可指定；</li>
 *   <li><b>清空最后节点保护</b>：把某能力排空到「集群 desired-ACTIVE 节点数为 0
 *       且<b>其他</b>节点仍 actual-ACTIVE」时抛 {@link IllegalStateTransitionException}
 *       拒绝（他节点数据未及迁走、依赖未断）；其余节点均已实际停止后排空最后节点
 *       是明确的用户意图（排空=丢弃缓存数据），允许——此时调度器走「无处可迁直接
 *       排空停止 + 清空放置表」路径。全部 DISABLE 后任意设置（重新启用等）同样允许。</li>
 * </ul>
 */
public final class DesiredStateWriter {

    private final CapabilityStateStore store;

    public DesiredStateWriter(CapabilityStateStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /**
     * 写入期望态（带合法性校验）。
     *
     * @throws IllegalArgumentException              desired 非 ACTIVE/DISABLED 或参数为 null
     * @throws IllegalStateTransitionException      他节点仍在服务时拒绝排空最后节点
     */
    public void setDesired(NodeId node, CapabilityType type, CapabilityState desired) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(desired, "desired");
        validateDesired(desired);
        if (desired == CapabilityState.DISABLED && wouldEmptyLastActiveNode(node, type)) {
            throw new IllegalStateTransitionException("ACTIVE", "DISABLED",
                    "refusing to disable the last desired-ACTIVE node for " + type
                            + " while other actual-ACTIVE nodes still exist: " + node);
        }
        store.setDesired(node, type, desired);
    }

    /**
     * 写入<b>初始</b>期望态（首启引导）：仅当该 (node, type) 尚无任何记录时写入
     * env 配置的默认值；已有记录（管理面经 {@link #setDesired} 显式设置过，
     * 含 DISABLED）则<b>不覆盖</b>——否则「UI 禁用节点 → 重启被 env 复活」
     * 破坏期望态的持久化语义（env 是首启默认值，非每次启动的强制值）。
     *
     * @return 是否实际写入（false=已有记录，保持现状）
     */
    public boolean setInitialDesired(NodeId node, CapabilityType type, CapabilityState desired) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(type, "type");
        validateDesired(desired);
        if (store.get(node, type).isPresent()) {
            return false; // 管理面已设置过：env 默认值不覆盖
        }
        store.setDesired(node, type, desired); // upsert：以 desired + actual=DISABLED 创建
        return true;
    }

    private static void validateDesired(CapabilityState desired) {
        if (desired != CapabilityState.ACTIVE && desired != CapabilityState.DISABLED) {
            throw new IllegalArgumentException(
                    "desired must be ACTIVE or DISABLED, got " + desired);
        }
    }

    /**
     * 禁用该节点后，该能力将无任何 desired-ACTIVE 节点（用于退役准入校验）。
     *
     * <p>注意排除 node 自身——它是被排空的目标，自身的 actual-ACTIVE 不是保护理由
     * （否则「全禁用」的最后一步永远被拒，用户明确的排空意图无法达成）。</p>
     */
    public boolean isLastDesiredActiveNode(NodeId node, CapabilityType type) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(type, "type");
        return wouldEmptyLastActiveNode(node, type);
    }

    /**
     * 排空最后节点保护：禁用 {@code node} 后集群将无 desired-ACTIVE 节点，
     * 且<b>其他</b>节点仍 actual-ACTIVE（数据未及迁走/依赖未断）→ 拒绝。
     * 注意排除 node 自身——它是被排空的目标，自身的 actual-ACTIVE 不是保护理由
     * （否则「全禁用」的最后一步永远被拒，用户明确的排空意图无法达成）。
     */
    private boolean wouldEmptyLastActiveNode(NodeId node, CapabilityType type) {
        List<CapabilityRecord> records = store.list();
        boolean otherDesiredActive = records.stream()
                .anyMatch(record -> record.type() == type
                        && !record.node().equals(node)
                        && record.desired() == CapabilityState.ACTIVE);
        if (otherDesiredActive) {
            return false;
        }
        return records.stream()
                .anyMatch(record -> record.type() == type
                        && !record.node().equals(node)
                        && record.actual() == CapabilityState.ACTIVE);
    }
}
