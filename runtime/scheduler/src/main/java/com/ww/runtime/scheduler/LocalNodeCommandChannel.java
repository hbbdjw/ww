package com.ww.runtime.scheduler;

import com.ww.runtime.capability.CapabilityManager;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * {@link NodeCommandChannel} 的本节点实现：直接委托进程内 {@link CapabilityManager}。
 *
 * <p>仅当 {@code node == self} 时生效，其余节点一律返回 {@code false}
 * （本实现无法触达远端节点）。命令失败（状态机拒绝/模块钩子异常）吞掉异常返回
 * {@code false}，由调度器下周期重试。</p>
 */
public final class LocalNodeCommandChannel implements NodeCommandChannel {

    private static final Logger LOG = LoggerFactory.getLogger(LocalNodeCommandChannel.class);

    private final NodeId self;
    private final CapabilityManager manager;

    public LocalNodeCommandChannel(NodeId self, CapabilityManager manager) {
        this.self = Objects.requireNonNull(self, "self");
        this.manager = Objects.requireNonNull(manager, "manager");
    }

    @Override
    public boolean startCapability(NodeId node, CapabilityType type) {
        if (!self.equals(node)) {
            LOG.debug("local channel cannot command remote node {} (self={})", node, self);
            return false;
        }
        try {
            manager.convergeStart(type);
            return true;
        } catch (Exception e) {
            LOG.warn("start capability {} on {} failed: {}", type, self, String.valueOf(e.getMessage()));
            return false;
        }
    }

    @Override
    public boolean stopCapability(NodeId node, CapabilityType type) {
        if (!self.equals(node)) {
            LOG.debug("local channel cannot command remote node {} (self={})", node, self);
            return false;
        }
        try {
            manager.convergeStop(type); // 幂等收敛：DISABLED 直接返回；ACTIVE 走 drain+stop 链
            return true;
        } catch (Exception e) {
            LOG.warn("stop capability {} on {} failed: {}", type, self, String.valueOf(e.getMessage()));
            return false;
        }
    }

    @Override
    public boolean freeze(NodeId node, CapabilityType type, boolean frozen) {
        if (!self.equals(node)) {
            LOG.debug("local channel cannot command remote node {} (self={})", node, self);
            return false;
        }
        try {
            manager.freeze(type, frozen);
            return true;
        } catch (Exception e) {
            LOG.warn("freeze({}) capability {} on {} failed: {}", frozen, type, self,
                    String.valueOf(e.getMessage()));
            return false;
        }
    }
}
