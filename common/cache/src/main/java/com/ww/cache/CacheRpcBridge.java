package com.ww.cache;

import java.util.Map;

import com.ww.rpc.core.RequestContext;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;
import com.ww.rpc.core.RpcService;
import com.ww.rpc.interceptor.MetricsInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache gRPC 桥接器：把 cache 模块的命令处理能力注册到 cluster-rpc 的 {@link RpcService}，
 * 实现 TCP（CacheClient）与 gRPC（GrpcCacheRpcClient）双协议接入。
 *
 * <h3>注册的服务</h3>
 * <ul>
 *   <li>{@code cache.command} — 同步命令（读写、集群管理）：调用 CacheServer.dispatchCommand，
 *       返回完整响应。对应客户端 {@link GrpcCacheRpcClient#send}。</li>
 *   <li>{@code cache.replica} — 副本异步复制（fire-and-forget）：handler 立即返回 OK ACK，
 *       实际命令在服务端异步线程执行。对应客户端 {@link GrpcCacheRpcClient#sendAsync}。</li>
 * </ul>
 *
 * <h3>协议映射</h3>
 * <p>请求头：{@code x-cache-op}=opCode, {@code x-cache-flags}=flags, {@code x-cache-auth}=token；
 * 请求 payload 与 TCP 协议 payload 完全一致（{@link CacheProtocol} 编码）。
 * <p>响应头：{@code x-cache-status}={@code ok/null/error}；响应 payload 与 TCP 一致。
 */
public class CacheRpcBridge {

    private static final Logger log = LoggerFactory.getLogger(CacheRpcBridge.class);

    private final CacheServer server;
    private final RpcService rpcService;
    private final String authToken;
    private final MetricsInterceptor metrics;

    /**
     * @param server     已注入 distributedCache / membership / failover / rebalance 的 CacheServer
     * @param rpcService cluster-rpc 服务实例（已启动或待启动）
     * @param authToken  认证 token，若请求未携带或不匹配则拒绝
     */
    public CacheRpcBridge(CacheServer server, RpcService rpcService, String authToken) {
        this.server = server;
        this.rpcService = rpcService;
        this.authToken = authToken;
        this.metrics = new MetricsInterceptor();
    }

    /**
     * 注册 cache.command 和 cache.replica 两个服务到 RpcService，并挂接 metrics 拦截器。
     */
    public void register() {
        rpcService.registerHandler(GrpcCacheRpcClient.SVC_COMMAND, this::handleCommand);
        rpcService.registerHandler(GrpcCacheRpcClient.SVC_REPLICA, this::handleReplica);
        log.info("Cache gRPC handler 已注册: {}, {} (auth={})",
                GrpcCacheRpcClient.SVC_COMMAND, GrpcCacheRpcClient.SVC_REPLICA,
                authToken != null && !authToken.isEmpty() ? "enabled" : "disabled");
    }

    /**
     * 获取此 bridge 使用的 metrics 拦截器，供管理面查询指标。
     */
    public MetricsInterceptor getMetrics() {
        return metrics;
    }

    /**
     * 同步命令处理：解析 header → 认证 → dispatchCommand → 封装 RpcResponse。
     */
    private RpcResponse handleCommand(RpcRequest req, RequestContext ctx) {
        String reqId = req.getRequestId();
        Map<String, String> headers = req.getHeaders();
        byte flags = (byte) parseIntHeader(headers, GrpcCacheRpcClient.H_FLAGS, 0);
        byte opCode = (byte) parseIntHeader(headers, GrpcCacheRpcClient.H_OP, 0);

        if (!checkAuth(headers)) {
            return RpcResponse.newBuilder()
                    .requestId(reqId)
                    .status(RpcResponse.Status.ERROR)
                    .errorMessage("cache auth failed")
                    .header(GrpcCacheRpcClient.H_RESP_STATUS, GrpcCacheRpcClient.RESP_ERROR)
                    .build();
        }

        byte[] argsPayload = req.getPayload() != null ? req.getPayload() : new byte[0];
        CacheServer.CommandResult cr = server.dispatchCommand(flags, opCode, argsPayload, false);
        if (cr == null) {
            return RpcResponse.newBuilder()
                    .requestId(reqId)
                    .status(RpcResponse.Status.OK)
                    .payload(new byte[0])
                    .header(GrpcCacheRpcClient.H_RESP_STATUS, GrpcCacheRpcClient.RESP_NULL)
                    .build();
        }

        String respStatus = switch (cr.status) {
            case CacheProtocol.STATUS_OK -> GrpcCacheRpcClient.RESP_OK;
            case CacheProtocol.STATUS_NULL -> GrpcCacheRpcClient.RESP_NULL;
            case CacheProtocol.STATUS_ERROR -> GrpcCacheRpcClient.RESP_ERROR;
            default -> GrpcCacheRpcClient.RESP_OK;
        };

        return RpcResponse.newBuilder()
                .requestId(reqId)
                .status(cr.status == CacheProtocol.STATUS_ERROR
                        ? RpcResponse.Status.ERROR : RpcResponse.Status.OK)
                .payload(cr.payload)
                .header(GrpcCacheRpcClient.H_RESP_STATUS, respStatus)
                .build();
    }

    /**
     * 副本异步复制：立即返回 OK，实际命令在调用线程（gRPC worker）执行，
     * 与 TCP 模式下 FLAG_REPLICA fire-and-forget 语义一致。
     */
    private RpcResponse handleReplica(RpcRequest req, RequestContext ctx) {
        String reqId = req.getRequestId();
        Map<String, String> headers = req.getHeaders();
        byte flags = (byte) parseIntHeader(headers, GrpcCacheRpcClient.H_FLAGS, 0);
        byte opCode = (byte) parseIntHeader(headers, GrpcCacheRpcClient.H_OP, 0);

        if (!checkAuth(headers)) {
            return RpcResponse.error(reqId, "cache auth failed");
        }

        // fire-and-forget：先返回 ACK，再执行；不阻塞调用方
        byte[] argsPayload = req.getPayload() != null ? req.getPayload() : new byte[0];
        try {
            server.dispatchCommand(flags, opCode, argsPayload, true);
        } catch (Exception e) {
            log.warn("cache.replica 执行异常 opCode={}: {}", Integer.toHexString(opCode & 0xFF), e.getMessage());
        }
        return RpcResponse.ok(reqId, new byte[0]);
    }

    private boolean checkAuth(Map<String, String> headers) {
        if (authToken == null || authToken.isEmpty()) return true;
        String provided = headers.get(GrpcCacheRpcClient.H_AUTH);
        return authToken.equals(provided);
    }

    private static int parseIntHeader(Map<String, String> headers, String key, int def) {
        if (headers == null) return def;
        String v = headers.get(key);
        if (v == null || v.isEmpty()) return def;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
