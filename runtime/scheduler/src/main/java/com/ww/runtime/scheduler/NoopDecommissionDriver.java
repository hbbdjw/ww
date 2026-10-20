package com.ww.runtime.scheduler;

/**
 * {@link DecommissionDriver} 空实现：测试兜底 / 未装配退役功能时的默认值。
 */
public final class NoopDecommissionDriver implements DecommissionDriver {

    @Override
    public void advance() {
        // no-op
    }
}
