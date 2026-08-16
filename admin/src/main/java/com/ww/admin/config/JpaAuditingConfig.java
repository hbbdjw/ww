package com.ww.admin.config;

import java.util.Optional;

import org.apache.shiro.SecurityUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.AuditorAware;

/**
 * JPA 审计配置：提供 {@code auditorAware} Bean，
 * 与 {@code AdminApplication} 上 {@code @EnableJpaAuditing(auditorAwareRef = "auditorAware")} 对应。
 * <p>审计人为当前 Shiro 登录用户名；未登录场景（如启动初始化、定时任务）回退为 "system"。
 */
@Configuration
public class JpaAuditingConfig {

    /**
     * 未登录时的默认审计人
     */
    private static final String SYSTEM_AUDITOR = "system";

    @Bean
    public AuditorAware<String> auditorAware() {
        return () -> {
            try {
                Object principal = SecurityUtils.getSubject().getPrincipal();
                if (principal != null) {
                    return Optional.of(principal.toString());
                }
            } catch (Exception ignore) {
                // Shiro 环境未就绪或非 Web 线程，回退为系统审计人
            }
            return Optional.of(SYSTEM_AUDITOR);
        };
    }
}
