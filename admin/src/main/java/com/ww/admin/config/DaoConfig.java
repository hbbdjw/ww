package com.ww.admin.config;

import com.ww.common.dao.BaseDaoImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * DAO配置 — 创建共享的通用BaseDaoImpl实例，适配任意Entity类型
 */
@Configuration
public class DaoConfig {

    @Bean
    public BaseDaoImpl sharedDao() {
        return new BaseDaoImpl();
    }
}
