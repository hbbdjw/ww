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
 *       且仍有 actual-ACTIVE 节点」时抛 {@link IllegalStateTransitionException} 拒绝
 *       （无处可迁、服务会直接中断）；全部节点已 DISABLE 后则允许任意设置
 *       （重新启用等）。</li>
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
     * @throws IllegalStateTransitionException      拒绝清空最后一个 desired-ACTIVE 节点
     */
    public void setDesired(NodeId node, CapabilityType type, CapabilityState desired) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(desired, "desired");
        if (desired != CapabilityState.ACTIVE && desired != CapabilityState.DISABLED) {
            throw new IllegalArgumentException(
                    "desired must be ACTIVE or DISABLED, got " + desired);
        }
        if (desired == CapabilityState.DISABLED && wouldEmptyLastActiveNode(node, type)) {
            throw new IllegalStateTransitionException("ACTIVE", "DISABLED",
                    "refusing to disable the last desired-ACTIVE node for " + type
                            + " while actual-ACTIVE nodes still exist: " + node);
        }
        store.setDesired(node, type, desired);
    }

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
                        && record.actual() == CapabilityState.ACTIVE);
    }
}
