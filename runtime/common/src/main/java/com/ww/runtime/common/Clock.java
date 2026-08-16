package com.ww.runtime.common;

/**
 * 时钟抽象：所有超时/租约/心跳计时必须经由 Clock，禁止直接调用系统时间 API。
 */
public interface Clock {

    long nowMillis();
}
