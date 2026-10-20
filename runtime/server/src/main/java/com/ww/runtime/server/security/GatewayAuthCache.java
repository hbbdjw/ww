package com.ww.runtime.server.security;

import com.ww.runtime.data.cache.CacheGateway;
import org.springframework.stereotype.Component;

/**
 * {@link AuthCachePort} 的集群 CACHE 网关适配：委托 {@link CacheGateway}
 * 完成 KV 读写删（值为 Map 时由网关 JSON 编解码，读路径严格 read-after-write）。
 *
 * <p>不吞异常：网关抛出的能力未就绪/fencing 等异常原样传播，
 * 由登录/鉴权链路按 fail-closed 处理。</p>
 */
@Component
public class GatewayAuthCache implements AuthCachePort {

    private final CacheGateway gateway;

    public GatewayAuthCache(CacheGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public void put(String key, Object value, long ttlMillis) {
        gateway.put(key, value, ttlMillis);
    }

    @Override
    public Object get(String key) {
        return gateway.get(key);
    }

    @Override
    public void remove(String key) {
        gateway.remove(key);
    }
}
