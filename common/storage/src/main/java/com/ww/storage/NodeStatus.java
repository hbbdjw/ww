package com.ww.storage;

/**
 * 存储节点健康状态机（failover 用）。
 *
 * <h3>状态流转</h3>
 * <ul>
 *   <li>{@link #ALIVE} → {@link #SUSPECT}：心跳连续失败 2 次</li>
 *   <li>{@link #SUSPECT} → {@link #DEAD}：再失败 1 次（累计 3 次），触发 failover</li>
 *   <li>{@link #SUSPECT} → {@link #ALIVE}：心跳恢复</li>
 *   <li>{@link #DEAD} → {@link #QUARANTINE}：心跳恢复，先隔离等反向同步</li>
 *   <li>{@link #QUARANTINE} → {@link #ALIVE}：反向 reconcile 校验通过</li>
 * </ul>
 *
 * <h3>路由过滤</h3>
 * 仅 {@link #ALIVE}/{@link #SUSPECT} 纳入 {@link NodeRouter}；
 * {@link #DEAD}/{@link #QUARANTINE} 不路由（failover 后 primary 已切到副本，
 * QUARANTINE 期间原主不接路由，等反向补齐数据后恢复）。
 */
public enum NodeStatus {
    /** 健康，纳入路由 */
    ALIVE,
    /** 怀疑故障（心跳失败 1-2 次），仍纳入路由 */
    SUSPECT,
    /** 已确认故障（心跳失败 ≥3 次），不纳入路由，primary 已 failover 到副本 */
    DEAD,
    /** 恢复中（原 DEAD 节点心跳恢复），不纳入路由，等反向 reconcile 校验通过转 ALIVE */
    QUARANTINE;

    /** 是否纳入路由（ALIVE / SUSPECT） */
    public boolean isRoutable() {
        return this == ALIVE || this == SUSPECT;
    }
}
