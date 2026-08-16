package com.ww.rpc.core;

import java.util.Collections;
import java.util.Map;

/**
 * RequestContext 默认实现。
 */
public class DefaultRequestContext implements RequestContext {

    private final String remoteAddress;
    private final long remainingTimeoutMs;
    private final Map<String, String> headers;

    public DefaultRequestContext(String remoteAddress, long remainingTimeoutMs, Map<String, String> headers) {
        this.remoteAddress = remoteAddress;
        this.remainingTimeoutMs = remainingTimeoutMs;
        this.headers = headers == null ? Map.of() : Collections.unmodifiableMap(headers);
    }

    public DefaultRequestContext() {
        this("local", -1, Map.of());
    }

    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public long getRemainingTimeoutMs() {
        return remainingTimeoutMs;
    }

    @Override
    public String getHeader(String key) {
        return headers.get(key);
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }
}
