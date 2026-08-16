package com.ww.admin.security;

import com.ww.admin.service.AccessTokenService;
import com.ww.admin.service.UserService;
import com.ww.cache.Cache;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.credential.DefaultPasswordService;
import org.apache.shiro.authc.credential.PasswordService;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.session.mgt.eis.SessionDAO;
import org.apache.shiro.spring.security.interceptor.AuthorizationAttributeSourceAdvisor;
import org.apache.shiro.spring.web.config.DefaultShiroFilterChainDefinition;
import org.apache.shiro.spring.web.config.ShiroFilterChainDefinition;
import org.apache.shiro.web.mgt.DefaultWebSecurityManager;
import org.apache.shiro.web.session.mgt.DefaultWebSessionManager;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Shiro 配置：基于数据库的认证与授权（取代 ini 配置），集成分布式缓存实现集群共享。
 * <p>
 * 要点：
 * <ul>
 *   <li>{@link DbRealm} 从数据库加载用户、角色、权限映射。</li>
 *   <li>{@link DualAuthFilter} 同时支持 Session 与 Token 两种认证方式。</li>
 *   <li>{@link PasswordService} 使用 Shiro 默认的 SHA-256 + 随机盐方案。</li>
 *   <li>权限映射全部持久化在数据库（sys_menu / sys_role_menu / sys_user_role）。</li>
 *   <li><b>集群共享</b>：Session、Token、授权信息均通过分布式缓存跨节点共享。</li>
 * </ul>
 * <p>
 * 集群会话共享：
 * <ul>
 *   <li>{@link CacheSessionDAO} 将 Session 存入分布式缓存，任意节点可读取其他节点创建的会话。</li>
 *   <li>Session TTL = globalSessionTimeout，过期自动清理。</li>
 * </ul>
 * <p>
 * 集群授权共享：
 * <ul>
 *   <li>{@link ShiroDistributedCacheManager} 装配到 SecurityManager，Shiro 自动注入 Realm。</li>
 *   <li>授权信息（角色+权限）缓存键为 {@code shiro:authz:<username>}，集群内共享。</li>
 *   <li>TTL 30 分钟安全网 + {@link AuthzCacheService} 主动失效双保险。</li>
 * </ul>
 * <p>
 * 采用 Shiro 3.0 推荐方式：提供 {@link ShiroFilterChainDefinition} 和自定义 Filter Bean，
 * 由 Shiro Spring Boot Starter 的自动配置装配 {@code ShiroFilterFactoryBean}。
 */
@Configuration
public class ShiroConfig {

    /**
     * 密码服务：默认 SHA-256 + 随机盐（盐内嵌于哈希串，无需单独字段）。
     */
    @Bean
    public PasswordService passwordService() {
        return new DefaultPasswordService();
    }

    /**
     * 数据库 Realm。
     */
    @Bean
    public Realm dbRealm(UserService userService,
                         AccessTokenService accessTokenService,
                         PasswordService passwordService) {
        return new DbRealm(userService, accessTokenService, passwordService);
    }

    /**
     * 基于分布式缓存的 SessionDAO：实现集群会话共享。
     * <p>会话以 {@code shiro:session:<sessionId>} 为键存入分布式缓存，
     * TTL = globalSessionTimeout，任意节点可读写。
     */
    @Bean
    public SessionDAO sessionDAO(Cache<String, Object> distributedCache) {
        return new CacheSessionDAO(distributedCache);
    }

    /**
     * Session 管理器：30 分钟超时，Cookie 名 ww_session，使用分布式 SessionDAO。
     */
    @Bean
    public DefaultWebSessionManager sessionManager(SessionDAO sessionDAO) {
        DefaultWebSessionManager sessionManager = new DefaultWebSessionManager();
        // 30 分钟超时
        sessionManager.setGlobalSessionTimeout(30 * 60 * 1000L);
        // 定时清理过期会话（CacheSessionDAO.getActiveSessions 返回空，验证调度器不会发现过期会话，
        // 实际过期由缓存 TTL 自动保障）
        sessionManager.setDeleteInvalidSessions(true);
        sessionManager.setSessionValidationSchedulerEnabled(true);
        // 集群 SessionDAO
        sessionManager.setSessionDAO(sessionDAO);
        org.apache.shiro.web.servlet.SimpleCookie cookie = new org.apache.shiro.web.servlet.SimpleCookie("ww_session");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(30 * 60);
        sessionManager.setSessionIdCookie(cookie);
        return sessionManager;
    }

    /**
     * 安全管理器：装配 Realm、SessionManager 和分布式 CacheManager。
     * <p>CacheManager 注入后 Shiro 自动将其传递给 Realm，启用授权缓存集群共享。
     */
    @Bean
    public DefaultWebSecurityManager securityManager(Realm dbRealm,
                                                      DefaultWebSessionManager sessionManager,
                                                      ShiroDistributedCacheManager shiroCacheManager) {
        DefaultWebSecurityManager securityManager = new DefaultWebSecurityManager();
        securityManager.setRealm(dbRealm);
        securityManager.setSessionManager(sessionManager);
        // 装配分布式缓存管理器：授权信息集群共享
        securityManager.setCacheManager(shiroCacheManager);
        return securityManager;
    }

    /**
     * 自定义双模认证过滤器（Bean 名 "dual" 对应过滤器链中的 "dual" 关键字）。
     */
    @Bean("dual")
    public DualAuthFilter dualAuthFilter() {
        return new DualAuthFilter();
    }

    /**
     * 禁止 Spring Boot 将 {@link DualAuthFilter} 自动注册为 Servlet Filter。
     */
    @Bean
    public FilterRegistrationBean<DualAuthFilter> dualAuthFilterRegistration(DualAuthFilter dualAuthFilter) {
        FilterRegistrationBean<DualAuthFilter> registration = new FilterRegistrationBean<>(dualAuthFilter);
        registration.setEnabled(false);
        return registration;
    }

    /**
     * 过滤器链定义：定义各路径的认证策略。
     */
    @Bean
    public ShiroFilterChainDefinition shiroFilterChainDefinition() {
        DefaultShiroFilterChainDefinition chainDefinition = new DefaultShiroFilterChainDefinition();
        chainDefinition.addPathDefinition("/auth/login", "anon");
        chainDefinition.addPathDefinition("/auth/token", "anon");
        chainDefinition.addPathDefinition("/auth/logout", "dual");
        chainDefinition.addPathDefinition("/error", "anon");
        // 节点间内部通信端点放行（Controller 内部通过 X-Storage-Token 自行校验）
        chainDefinition.addPathDefinition("/internal/**", "anon");
        chainDefinition.addPathDefinition("/**", "dual");
        return chainDefinition;
    }

    /**
     * 启用 Shiro 注解（@RequiresPermissions / @RequiresRoles）。
     */
    @Bean
    public AuthorizationAttributeSourceAdvisor authorizationAttributeSourceAdvisor(SecurityManager securityManager) {
        AuthorizationAttributeSourceAdvisor advisor = new AuthorizationAttributeSourceAdvisor();
        advisor.setSecurityManager(securityManager);
        return advisor;
    }

    /**
     * 将 SecurityManager 绑定为 VM 静态单例，供 SSE 等异步线程回退获取。
     * <p>Shiro starter 默认仅按请求把 SecurityManager/Subject 绑定到 ThreadContext；而 SSE 长连接的
     * 异步 dispatch（SseEmitter 事件推送/心跳时 Tomcat 重新进入 servlet 链）发生在新线程，ThreadContext
     * 为空，此时 {@code ShiroHttpServletRequest.getSubject()} 会抛
     * {@code UnavailableSecurityManagerException} 导致推送失败。绑定 VM 单例后异步线程可正常获取
     * SecurityManager；普通请求仍优先使用 ThreadContext 绑定的 Subject，行为不变。
     */
    @Bean
    public SmartInitializingSingleton bindSecurityManagerSingleton(SecurityManager securityManager) {
        return () -> SecurityUtils.setSecurityManager(securityManager);
    }
}
