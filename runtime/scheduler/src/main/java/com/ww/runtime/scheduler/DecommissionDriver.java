package com.ww.runtime.scheduler;

/**
 * 退役流程推进端口（仅 leader 调用）。
 *
 * <p>由 {@link ReconcileLoop} 在每个调谐周期末尾调用一次；
 * 无活动退役记录时实现应为空操作。</p>
 */
public interface DecommissionDriver {

    /** 推进全部活动退役记录的状态机。异常由调用方吞掉记日志。 */
    void advance();
}
