package com.ww.admin.security;

import com.ww.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 授权缓存失效服务：在角色/权限/用户变更时主动清除分布式缓存中的授权信息。
 * <p>
 * 满足 read-after-write 一致性：权限变更后立即移除缓存条目，下次访问重新从数据库加载。
 * <p>
 * 缓存键格式：{@code shiro:authz:<username>}（与 {@link ShiroDistributedCacheManager#getCache}
 * 的前缀约定一致）。TTL（30 分钟）作为安全网兜底，但主路径依赖此处的主动失效。
 */
@Service
public class AuthzCacheService {

    private static final Logger log = LoggerFactory.getLogger(AuthzCacheService.class);

    private final Cache<String, Object> distributedCache;

    public AuthzCacheService(Cache<String, Object> distributedCache) {
        this.distributedCache = distributedCache;
    }

    /**
     * 失效指定用户的授权缓存。
     *
     * @param username 用户名（Shiro principal）
     */
    public void invalidate(String username) {
        if (username == null || username.isEmpty()) {
            return;
        }
        String key = ShiroDistributedCacheManager.AUTHZ_CACHE_PREFIX + username;
        distributedCache.remove(key);
        log.debug("已失效用户授权缓存: {}", username);
    }

    /**
     * 批量失效多个用户的授权缓存（角色权限变更时影响该角色下所有用户）。
     *
     * @param usernames 用户名列表
     */
    public void invalidateAll(List<String> usernames) {
        if (usernames == null || usernames.isEmpty()) {
            return;
        }
        for (String username : usernames) {
            invalidate(username);
        }
    }

    /**
     * 失效全部授权缓存（谨慎使用，仅在重大权限结构调整时调用）。
     */
    public void invalidateAll() {
        for (String key : distributedCache.keys()) {
            if (key.startsWith(ShiroDistributedCacheManager.AUTHZ_CACHE_PREFIX)) {
                distributedCache.remove(key);
            }
        }
        log.info("已清空全部授权缓存");
    }
}
