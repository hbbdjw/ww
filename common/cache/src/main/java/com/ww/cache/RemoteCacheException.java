package com.ww.cache;

import java.io.IOException;

/**
 * 远端缓存服务端返回 STATUS_ERROR 时抛出。
 *
 * <p>继承 {@link IOException} 以便调用方按统一方式处理；
 * 但 {@link CacheClient#send} 的重试逻辑会识别本类并跳过重试——
 * 服务端业务错误（如 "未启用 membership"、参数非法、key 不存在等）是确定性的，
 * 重试只会得到相同结果，且会丢失原始错误消息（被包装成 "连续 N 次失败"）。
 *
 * <p>与之相对，纯网络错误（连接拒绝、socket reset、read timeout）仍走重试，
 * 因为这些可能是瞬时故障（如对端正在重启、TCP backlog 未消费）。
 */
public class RemoteCacheException extends IOException {

    public RemoteCacheException(String message) {
        super(message);
    }

    public RemoteCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
