package com.ww.common;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 缓存端口和认证配置，绑定 application.yml 中 cache.*
 */
@Component
@ConfigurationProperties(prefix = "cache")
public class CachePortProperties {

    /** TCP 监听端口，默认 8700 */
    private int port = 8700;

    /** 节点间认证 token，为空则不启用认证 */
    private String authToken;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }
}
