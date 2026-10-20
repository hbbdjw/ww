package com.ww.runtime.common;

/**
 * 半同步复制超时：主写落盘后，在配置的超时时间内未获得任何存活副本的确认。
 *
 * <p>仅在 fail-closed 模式（{@code runtime.capabilities.storage-semi-sync-fail-closed=true}）
 * 下抛出并拒绝应答客户端——宁可写失败也不应答「可能随主节点死亡而丢失」的请求；
 * 降级模式（默认）不抛异常，改为记录 at-risk 观测项后照常应答。</p>
 *
 * <p>经全局异常处理器映射为 503（{@link RuntimeFrameworkException} 既有语义）。</p>
 */
public class ReplicationTimeoutException extends RuntimeFrameworkException {

    public ReplicationTimeoutException(String message) {
        super(message);
    }
}
