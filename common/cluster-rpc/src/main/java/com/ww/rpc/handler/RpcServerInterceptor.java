package com.ww.rpc.handler;

import com.ww.rpc.core.RequestContext;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;

/**
 * RPC 服务端拦截器：在请求被 handler 处理前后执行横切逻辑，
 * 如认证、日志、metrics、限流等。
 */
public interface RpcServerInterceptor {

    /**
     * 拦截请求。
     *
     * @param request  请求
     * @param ctx      请求上下文
     * @param invoker  后续调用链（继续向下执行）
     * @return 响应
     */
    RpcResponse intercept(RpcRequest request, RequestContext ctx, RpcInvoker invoker);

    /**
     * 调用链接口。
     */
    @FunctionalInterface
    interface RpcInvoker {
        RpcResponse invoke(RpcRequest request, RequestContext ctx);
    }
}
