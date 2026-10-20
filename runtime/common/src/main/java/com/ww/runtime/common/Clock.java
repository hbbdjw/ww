package com.ww.runtime.common;

/**
 * 时钟抽象：所有超时/租约/心跳计时必须经由 Clock，禁止直接调用系统时间 API。
 */
public interface Clock {

    long nowMillis();

    /**
     * 单调时间（毫秒）：不受 NTP 步进/回拨影响，用于租约到期等必须抵御墙钟跳变的判定。
     * 默认退化为墙钟以保持测试时钟（TestClock）的确定性语义。
     */
    default long monotonicMillis() {
        return nowMillis();
    }
}
