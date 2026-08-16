package com.ww.rpc.handler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;

import com.ww.rpc.core.DefaultRequestContext;
import com.ww.rpc.core.RequestContext;
import com.ww.rpc.core.RpcRequest;
import com.ww.rpc.core.RpcResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 请求处理器注册中心：维护 serviceName → handler 的映射，并提供统一分派入口。
 *
 * <p>支持通过 {@link #addInterceptor(RpcServerInterceptor)} 注册拦截器链，
 * 拦截器按注册顺序包裹 handler 调用（外层先执行）。
 *
 * <p>线程安全：所有读写基于 ConcurrentHashMap / CopyOnWriteArrayList。
 */
public class RequestHandlerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(RequestHandlerRegistry.class);

    private final Map<String, RpcRequestHandler> handlers = new ConcurrentHashMap<>();
    private final List<RpcServerInterceptor> interceptors = new CopyOnWriteArrayList<>();

    /**
     * 注册处理器。若 serviceName 已存在则覆盖。
     */
    public void register(String serviceName, RpcRequestHandler handler) {
        Objects.requireNonNull(serviceName, "serviceName");
        Objects.requireNonNull(handler, "handler");
        RpcRequestHandler prev = handlers.put(serviceName, handler);
        if (prev != null) {
            LOG.warn("覆盖已注册的 RPC handler: serviceName={}", serviceName);
        } else {
            LOG.debug("注册 RPC handler: serviceName={}", serviceName);
        }
    }

    /**
     * 注销处理器。
     */
    public RpcRequestHandler unregister(String serviceName) {
        return handlers.remove(serviceName);
    }

    /**
     * 添加拦截器（追加到链尾）。拦截器按添加顺序依次包裹调用。
     */
    public void addInterceptor(RpcServerInterceptor interceptor) {
        Objects.requireNonNull(interceptor, "interceptor");
        interceptors.add(interceptor);
    }

    /**
     * 移除拦截器。
     */
    public boolean removeInterceptor(RpcServerInterceptor interceptor) {
        return interceptors.remove(interceptor);
    }

    /**
     * 查找处理器。
     */
    public RpcRequestHandler lookup(String serviceName) {
        return handlers.get(serviceName);
    }

    /**
     * 是否已注册。
     */
    public boolean hasHandler(String serviceName) {
        return handlers.containsKey(serviceName);
    }

    /** @deprecated use {@link #hasHandler(String)} */
    @Deprecated
    public boolean isRegistered(String serviceName) {
        return hasHandler(serviceName);
    }

    /**
     * 分派并处理请求，经过拦截器链后到达目标 handler。
     *
     * @throws IllegalArgumentException 未注册的 serviceName
     */
    public RpcResponse handle(RpcRequest request) {
        Objects.requireNonNull(request, "request");
        String serviceName = request.getServiceName();
        RpcRequestHandler handler = handlers.get(serviceName);
        if (handler == null) {
            return RpcResponse.error(request.getRequestId(),
                    "no handler registered for service: " + serviceName);
        }
        DefaultRequestContext ctx = new DefaultRequestContext(
                request.getHeader("x-remote-addr"),
                -1,
                request.getHeaders());
        try {
            return buildChain(handler, 0).invoke(request, ctx);
        } catch (Exception e) {
            LOG.error("handler 执行异常 serviceName={}, requestId={}",
                    serviceName, request.getRequestId(), e);
            return RpcResponse.error(request.getRequestId(),
                    "handler error: " + e.getMessage());
        }
    }

    /**
     * 递归构建拦截器链：interceptors[0] → interceptors[1] → ... → handler。
     */
    private RpcServerInterceptor.RpcInvoker buildChain(RpcRequestHandler handler, int index) {
        RpcServerInterceptor.RpcInvoker terminal = (req, ctx) -> handler.handle(req, ctx);
        if (index >= interceptors.size()) {
            return terminal;
        }
        RpcServerInterceptor interceptor = interceptors.get(index);
        RpcServerInterceptor.RpcInvoker next = buildChain(handler, index + 1);
        return (req, ctx) -> interceptor.intercept(req, ctx, next);
    }

    /**
     * 获取所有已注册的服务名（不可变快照）。
     */
    public Set<String> listServices() {
        return Set.copyOf(handlers.keySet());
    }

    /** @deprecated use {@link #listServices()} */
    @Deprecated
    public Set<String> registeredServices() {
        return listServices();
    }

    /**
     * 清空所有处理器和拦截器。
     */
    public void clear() {
        handlers.clear();
        interceptors.clear();
    }
}
