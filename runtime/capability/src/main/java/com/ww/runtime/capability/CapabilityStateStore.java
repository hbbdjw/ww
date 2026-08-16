package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;

import java.util.List;
import java.util.Optional;

/**
 * 能力状态仓库：持久化"节点 × 能力"的期望态（desired，编排器意图）与
 * 实际态（actual，本节点状态机产出）。
 *
 * <p>desired 由控制面写入（编排器决定某节点应运行哪些能力）；
 * actual 由 {@link CapabilityManager} 在每次状态跃迁后写入。
 * 两者之差即为需要收敛的偏差。
 */
public interface CapabilityStateStore {

    /**
     * 读取单条记录。
     *
     * @return 记录；该节点未登记此能力时返回 {@link Optional#empty()}
     */
    Optional<CapabilityRecord> get(NodeId node, CapabilityType type);

    /**
     * 设置期望态。记录不存在时创建（actual 取默认 DISABLED）。
     */
    void setDesired(NodeId node, CapabilityType type, CapabilityState desired);

    /**
     * 设置实际态。记录不存在时创建（desired 取默认 DISABLED）。
     */
    void setActual(NodeId node, CapabilityType type, CapabilityState actual);

    /**
     * 列出全部记录（集群视图，控制面使用）。
     */
    List<CapabilityRecord> list();

    /**
     * 能力状态记录值对象。
     *
     * @param node             节点
     * @param type             能力类型
     * @param desired          期望态
     * @param actual           实际态
     * @param updatedAtMillis  最后更新时间（毫秒，来自注入的 Clock）
     */
    record CapabilityRecord(NodeId node,
                            CapabilityType type,
                            CapabilityState desired,
                            CapabilityState actual,
                            long updatedAtMillis) {
    }
}
