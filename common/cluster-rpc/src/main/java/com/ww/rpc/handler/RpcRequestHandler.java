package com.ww.rpc.handler;

import com.ww.rpc.core.RequestContext;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;

/**
 * RPC 请求处理器 SPI：各业务模块实现此接口将自己的业务操作暴露给 RPC 层。
 *
 * <p>使用示例：
 * <pre>
 * rpcService.registerHandler("cache.put", (req, ctx) -> {
 *     // 解析 req.getPayload()，执行 put 操作
 *     return RpcResponse.ok(req.getRequestId(), resultBytes);
 * });
 * </pre>
 */
@FunctionalInterface
public interface RpcRequestHandler {

    /**
     * 处理 RPC 请求。
     *
     * @param request RPC 请求
     * @param ctx     请求上下文（源节点、超时、headers 等）
     * @return RPC 响应
     */
    RpcResponse handle(RpcRequest request, RequestContext ctx);
}
