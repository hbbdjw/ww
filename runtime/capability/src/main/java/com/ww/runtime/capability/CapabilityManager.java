package com.ww.runtime.capability;

import java.util.EnumSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.ww.runtime.common.CapabilityState;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.IllegalStateTransitionException;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.RuntimeFrameworkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 能力管理器（单节点视角）：管理<b>本节点</b>上全部已注册 {@link CapabilityModule}
 * 的生命周期状态机。
 *
 * <p>每个命令方法 = 状态跃迁（校验静态跃迁表）+ 调用模块钩子 + 持久化 actual 到
 * {@link CapabilityStateStore}。非法跃迁抛 {@link IllegalStateTransitionException}
 * 且状态保持不变；模块钩子抛出的异常会把能力置为 FAILED（markFailed）并包装为
 * {@link RuntimeFrameworkException} 向上传播。
 *
 * <h2>静态跃迁表（唯一合法集）</h2>
 * <pre>
 *   DISABLED  → STARTING
 *   STARTING  → WARMING | FAILED
 *   WARMING   → ACTIVE  | FAILED
 *   ACTIVE    → DRAINING| FAILED
 *   DRAINING  → MIGRATING | ACTIVE | STOPPING | FAILED
 *   MIGRATING → STOPPING | FAILED
 *   STOPPING  → DISABLED | FAILED
 *   FAILED    → DISABLED
 * </pre>
 * 另有特殊规则：{@link #markFailed} 从任何<b>非 DISABLED</b> 状态可达 FAILED
 * （含 FAILED→FAILED 幂等）。
 *
 * <h2>并发语义</h2>
 * 命令来源天然多线程（gRPC 控制面、调度器重试）。同一能力的状态跃迁与钩子执行
 * 由 per-type 锁（{@link #lockFor}）串行化：{@link #start} 采用「锁外预读 + 锁内复核」，
 * 其余跃迁命令方法体整体持锁；不同能力类型互不阻塞。
 * {@link #convergeStart}/{@link #convergeStop} 是命令级幂等收敛入口，
 * 命令式方法（start/stop 等）语义保持不变。
 */
public final class CapabilityManager {

    private static final Logger LOG = LoggerFactory.getLogger(CapabilityManager.class);

    private static final Map<CapabilityState, Set<CapabilityState>> LEGAL_TRANSITIONS = Map.of(
            CapabilityState.DISABLED, EnumSet.of(CapabilityState.STARTING),
            CapabilityState.STARTING, EnumSet.of(CapabilityState.WARMING, CapabilityState.FAILED),
            CapabilityState.WARMING, EnumSet.of(CapabilityState.ACTIVE, CapabilityState.FAILED),
            CapabilityState.ACTIVE, EnumSet.of(CapabilityState.DRAINING, CapabilityState.FAILED),
            CapabilityState.DRAINING, EnumSet.of(CapabilityState.MIGRATING, CapabilityState.ACTIVE,
                    CapabilityState.STOPPING, CapabilityState.FAILED),
            CapabilityState.MIGRATING, EnumSet.of(CapabilityState.STOPPING, CapabilityState.FAILED),
            CapabilityState.STOPPING, EnumSet.of(CapabilityState.DISABLED, CapabilityState.FAILED),
            CapabilityState.FAILED, EnumSet.of(CapabilityState.DISABLED));

    private final NodeId self;
    private final CapabilityStateStore store;
    private final Clock clock;
    private final Map<CapabilityType, CapabilityModule> modules = new ConcurrentHashMap<>();
    /** 冻结前钩子（如 STORAGE 在 freeze 前排空副本复制 backlog，确保副本已追平）。 */
    private final Map<CapabilityType, Runnable> freezeHooks = new ConcurrentHashMap<>();
    /** per-type 锁：同一能力的跃迁/钩子串行化，不同能力互不阻塞（枚举 key，无膨胀）。 */
    private final Map<CapabilityType, Object> typeLocks = new ConcurrentHashMap<>();

    /**
     * @param self  本节点标识
     * @param store 状态仓库（actual 在每次跃迁后写入）
     * @param clock 时钟（用于仓库记录的 updatedAtMillis——由仓库实现消费）
     */
    public CapabilityManager(NodeId self, CapabilityStateStore store, Clock clock) {
        this.self = Objects.requireNonNull(self, "self");
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * 判断一条跃迁是否在静态跃迁表的合法集内。
     * 注意：{@link #markFailed} 的特殊规则（任何非 DISABLED→FAILED）不经过本表。
     */
    public static boolean isLegalTransition(CapabilityState from, CapabilityState to) {
        return LEGAL_TRANSITIONS.getOrDefault(Objects.requireNonNull(from), Set.of())
                .contains(Objects.requireNonNull(to));
    }

    /**
     * 注册能力模块。重复注册同一类型将覆盖旧实例；
     * 仓库中无该能力记录时初始化 desired=actual=DISABLED。
     */
    public void register(CapabilityModule module) {
        Objects.requireNonNull(module, "module");
        modules.put(module.type(), module);
        var existing = store.get(self, module.type());
        if (existing.isEmpty()) {
            store.setActual(self, module.type(), CapabilityState.DISABLED);
        } else if (existing.get().actual() != CapabilityState.DISABLED) {
            // 进程重启：本节点能力模块必然未运行，actual 归位 DISABLED（desired 保留），
            // 由调度器据 desired 重新拉起。遗留的 desired==actual==ACTIVE 若不归位，
            // 调度器会判定无需拉起，导致状态表显示 ACTIVE 而模块实际未启动
            // （actual 由本节点状态机自治产出）。
            store.setActual(self, module.type(), CapabilityState.DISABLED);
        }
    }

    /**
     * 已注册模块快照（类型 → 模块，不可变）。
     */
    public Map<CapabilityType, CapabilityModule> modules() {
        return Map.copyOf(modules);
    }

    /**
     * 查询本节点某能力的当前实际态；无记录时视为 DISABLED。
     */
    public CapabilityState state(CapabilityType type) {
        return store.get(self, type).map(CapabilityStateStore.CapabilityRecord::actual)
                .orElse(CapabilityState.DISABLED);
    }

    /**
     * 启动链条（同步完成）：DISABLED→STARTING（调 {@code start()}）→
     * WARMING（调 {@code warm()}）→ ACTIVE。任一钩子异常则 markFailed 并抛
     * {@link RuntimeFrameworkException}。
     */
    public void start(CapabilityType type) {
        CapabilityModule module = module(type);
        // 锁外预读（竞态双方须先各自读到当前态），再锁内复核：
        // 预读到跃迁完成之间状态被并发改变 → 竞态落败，抛非法跃迁，钩子不重复执行
        CapabilityState pre = state(type);
        synchronized (lockFor(type)) {
            if (state(type) != pre) {
                throw new IllegalStateTransitionException(state(type), CapabilityState.STARTING,
                        "capability " + type + " on " + self + ": state changed concurrently during start()");
            }
            transition(type, CapabilityState.STARTING);
            try {
                module.start();
                transition(type, CapabilityState.WARMING);
                module.warm();
                transition(type, CapabilityState.ACTIVE);
            } catch (Exception e) {
                markFailed(type);
                throw new RuntimeFrameworkException(
                        "capability " + type + " failed to start on " + self + ": " + e.getMessage(), e);
            }
        }
    }

    /**
     * 排水：ACTIVE→DRAINING，调 {@code drain()}（维护态：停新写、排空在途请求）。
     */
    public void drain(CapabilityType type) {
        CapabilityModule module = module(type);
        synchronized (lockFor(type)) {
            transition(type, CapabilityState.DRAINING);
            try {
                module.drain();
            } catch (Exception e) {
                markFailed(type);
                throw new RuntimeFrameworkException(
                        "capability " + type + " failed to drain on " + self + ": " + e.getMessage(), e);
            }
        }
    }

    /**
     * 开始迁移：DRAINING→MIGRATING。不调钩子——数据搬迁由 MigrationEngine
     * 用模块的 export/import API（参见 {@link MockCapability}）驱动。
     */
    public void beginMigrate(CapabilityType type) {
        module(type);
        synchronized (lockFor(type)) {
            transition(type, CapabilityState.MIGRATING);
        }
    }

    /**
     * 结束迁移：MIGRATING→STOPPING。之后调用 {@link #stop} 释放资源。
     */
    public void endMigrate(CapabilityType type) {
        module(type);
        synchronized (lockFor(type)) {
            requireFrom(type, CapabilityState.MIGRATING, CapabilityState.STOPPING, "endMigrate()");
            transition(type, CapabilityState.STOPPING);
        }
    }

    /**
     * 停止：
     * <ul>
     *   <li>STOPPING→DISABLED：调 {@code stop()} 后跃迁；</li>
     *   <li>DRAINING→STOPPING→DISABLED：排水未完成也可直接停止；</li>
     *   <li>FAILED→DISABLED：失败能力复位为干净态，<b>不调</b> {@code stop()}
     *       （模块可能已坏，清理由宿主负责）。</li>
     * </ul>
     * {@code stop()} 钩子异常则 markFailed（STOPPING→FAILED）并抛
     * {@link RuntimeFrameworkException}。
     */
    public void stop(CapabilityType type) {
        CapabilityModule module = module(type);
        synchronized (lockFor(type)) {
            CapabilityState from = state(type);
            if (from == CapabilityState.FAILED) {
                transition(type, CapabilityState.DISABLED);
                return;
            }
            if (from == CapabilityState.DRAINING) {
                transition(type, CapabilityState.STOPPING);
                from = CapabilityState.STOPPING;
            }
            if (from != CapabilityState.STOPPING) {
                throw new IllegalStateTransitionException(state(type), CapabilityState.DISABLED,
                        "stop() accepts STOPPING/DRAINING/FAILED only");
            }
            try {
                module.stop();
            } catch (Exception e) {
                markFailed(type);
                throw new RuntimeFrameworkException(
                        "capability " + type + " failed to stop on " + self + ": " + e.getMessage(), e);
            }
            transition(type, CapabilityState.DISABLED);
        }
    }

    /**
     * 收敛命令（幂等）：确保能力进入启动轨道。与 {@link #start} 的命令式语义互补：
     * <ul>
     *   <li>ACTIVE/WARMING/STARTING：已在轨道，幂等成功（不改状态、不进钩子）；</li>
     *   <li>FAILED：先 {@code stop} 复位为 DISABLED 再走 {@code start} 链；</li>
     *   <li>DRAINING：{@code cancelDrain} 恢复 ACTIVE；</li>
     *   <li>MIGRATING：拒绝（数据搬迁中，收敛方向由 {@link #convergeStop} 承担）；</li>
     *   <li>DISABLED：走 {@code start} 链。</li>
     * </ul>
     */
    public void convergeStart(CapabilityType type) {
        module(type);
        synchronized (lockFor(type)) {
            switch (state(type)) {
                case ACTIVE, WARMING, STARTING -> {
                    return;
                }
                case FAILED -> stop(type);            // FAILED→DISABLED 复位
                case DRAINING -> cancelDrain(type);   // DRAINING→ACTIVE 取消排水
                case MIGRATING -> throw new IllegalStateTransitionException(
                        CapabilityState.MIGRATING, CapabilityState.STARTING,
                        "convergeStart() rejects MIGRATING; use convergeStop()");
                default -> {
                }                                     // DISABLED 落到底部
            }
            if (state(type) == CapabilityState.DISABLED) {
                start(type);
            }
        }
    }

    /**
     * 收敛命令（幂等）：确保能力到达 DISABLED。与 {@link #stop} 的命令式语义互补：
     * <ul>
     *   <li>DISABLED：幂等成功；</li>
     *   <li>ACTIVE：{@code drain + stop} 链；</li>
     *   <li>DRAINING/STOPPING/FAILED：直接 {@code stop}；</li>
     *   <li>MIGRATING：{@code endMigrate + stop}；</li>
     *   <li>STARTING/WARMING：拒绝——启动轨道上的能力由 {@link #convergeStart}
     *       幂等确认收敛，stop 侧不得改状态。</li>
     * </ul>
     */
    public void convergeStop(CapabilityType type) {
        module(type);
        synchronized (lockFor(type)) {
            switch (state(type)) {
                case DISABLED -> {
                    return;
                }
                case ACTIVE -> drain(type);
                case MIGRATING -> endMigrate(type);
                case STARTING, WARMING -> throw new IllegalStateTransitionException(
                        state(type), CapabilityState.STOPPING,
                        "convergeStop() rejects " + state(type)
                                + "; capability on start track, converge via convergeStart()");
                default -> {
                }                                     // DRAINING/STOPPING/FAILED 直接 stop
            }
            stop(type);
        }
    }

    /**
     * 取消排水：DRAINING→ACTIVE（排水放弃，恢复服务）。
     */
    public void cancelDrain(CapabilityType type) {
        module(type);
        synchronized (lockFor(type)) {
            requireFrom(type, CapabilityState.DRAINING, CapabilityState.ACTIVE, "cancelDrain()");
            transition(type, CapabilityState.ACTIVE);
        }
    }

    /**
     * 冻结/解冻：仅透传模块钩子，<b>不改变</b>状态机状态。
     * 冻结前先执行已注册钩子（尽力而为，失败仅告警不阻断冻结——冻结本身是安全方向）。
     */
    public void freeze(CapabilityType type, boolean frozen) {
        if (frozen) {
            Runnable hook = freezeHooks.get(type);
            if (hook != null) {
                try {
                    hook.run();
                } catch (RuntimeException e) {
                    LOG.warn("pre-freeze hook failed for {}: {}", type, e.getMessage());
                }
            }
        }
        module(type).freeze(frozen);
    }

    /** 注册冻结前钩子（null 清除；典型：复制器 backlog 排空屏障）。 */
    public void setFreezeHook(CapabilityType type, Runnable hook) {
        if (hook == null) {
            freezeHooks.remove(type);
        } else {
            freezeHooks.put(type, hook);
        }
    }

    /**
     * 标记失败：任何非 DISABLED 状态→FAILED（FAILED→FAILED 幂等）；
     * DISABLED→FAILED 非法，抛 {@link IllegalStateTransitionException}。
     */
    public void markFailed(CapabilityType type) {
        module(type);
        synchronized (lockFor(type)) {
            CapabilityState from = state(type);
            if (from == CapabilityState.DISABLED) {
                throw new IllegalStateTransitionException(from, CapabilityState.FAILED,
                        "markFailed() is unreachable from DISABLED");
            }
            store.setActual(self, type, CapabilityState.FAILED);
        }
    }

    private void transition(CapabilityType type, CapabilityState to) {
        CapabilityState from = state(type);
        if (!isLegalTransition(from, to)) {
            throw new IllegalStateTransitionException(from, to,
                    "capability " + type + " on " + self);
        }
        store.setActual(self, type, to);
    }

    /**
     * 命令级来源校验：静态表允许一条边有多个来源（如 DRAINING→STOPPING、
     * WARMING→ACTIVE），但单个命令只接受定义中的唯一来源，防止借用其他合法边。
     */
    private void requireFrom(CapabilityType type, CapabilityState required,
                             CapabilityState to, String command) {
        CapabilityState from = state(type);
        if (from != required) {
            throw new IllegalStateTransitionException(from, to,
                    command + " requires " + required + " (capability " + type + " on " + self + ")");
        }
    }

    private CapabilityModule module(CapabilityType type) {
        Objects.requireNonNull(type, "type");
        CapabilityModule module = modules.get(type);
        if (module == null) {
            throw new NoSuchElementException("capability not registered on " + self + ": " + type);
        }
        return module;
    }

    /** per-type 锁对象（枚举 key，computeIfAbsent 无膨胀）。 */
    private Object lockFor(CapabilityType type) {
        return typeLocks.computeIfAbsent(Objects.requireNonNull(type, "type"), t -> new Object());
    }
}
