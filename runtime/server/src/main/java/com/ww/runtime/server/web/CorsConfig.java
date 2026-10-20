package com.ww.runtime.server.web;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * MVC 配置：仅保留异步支持超时。CORS 统一由 Spring Security 的
 * {@code CorsConfigurationSource} 处理（见
 * com.ww.runtime.server.security.SecurityConfig）。
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        // 大文件上传/下载异步请求超时：1 小时
        configurer.setDefaultTimeout(3_600_000L);
    }
}
