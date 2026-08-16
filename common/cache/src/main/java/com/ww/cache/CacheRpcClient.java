package com.ww.cache;

import java.io.IOException;

/**
 * 缓存节点间 RPC 客户端抽象接口。
 *
 * <p>解耦业务逻辑（DistributedCache/RemoteCache/FailoverManager 等）与底层传输协议
 * （自定义 TCP 二进制 / gRPC），支持协议切换而不侵入业务代码。
 *
 * <p>方法语义对应 {@link CacheProtocol} 二进制帧：
 * <ul>
 *   <li>{@link #send(byte, byte, byte[], String)}：同步发送命令并等待响应 payload；
 *       对 STATUS_ERROR 抛出 {@link RemoteCacheException}（与 TCP 实现一致，不做网络重试）</li>
 *   <li>{@link #sendAsync(byte, byte, byte[], String)}：fire-and-forget，用于 FLAG_REPLICA
 *       副本复制场景（服务端不回包，客户端不读响应）</li>
 * </ul>
 */
public interface CacheRpcClient extends AutoCloseable {

    /**
     * 同步发送命令并返回响应 payload。
     *
     * @param flags       帧标志（如 {@link CacheProtocol#FLAG_NORMAL} / {@link CacheProtocol#FLAG_REPLICA}）
     * @param opCode      操作码
     * @param argsPayload 编码后的参数 payload（含 opCode 前缀）
     * @param purpose     调用上下文描述（用于日志）
     * @return 响应 payload（已解帧，不含头部）；STATUS_NULL 返回零长数组
     * @throws IOException            网络错误
     * @throws RemoteCacheException   服务端业务错误（STATUS_ERROR），调用方不应重试
     */
    byte[] send(byte flags, byte opCode, byte[] argsPayload, String purpose) throws IOException;

    /**
     * Fire-and-forget 发送（副本复制等场景）。
     * 实现应确保数据已写入网络层即可返回，不阻塞等待响应。
     *
     * @param flags       帧标志（通常为 {@link CacheProtocol#FLAG_REPLICA}）
     * @param opCode      操作码
     * @param argsPayload 编码后的参数 payload
     * @param purpose     调用上下文描述
     * @throws IOException 网络写入失败
     */
    void sendAsync(byte flags, byte opCode, byte[] argsPayload, String purpose) throws IOException;

    /**
     * @return 远端主机地址
     */
    String getHost();

    /**
     * @return 远端端口
     */
    int getPort();

    /**
     * 释放底层资源（连接/线程池等），不抛出检查型异常。
     */
    void close();
}
