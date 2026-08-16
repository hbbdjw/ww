package com.ww.admin;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * @author Jw
 * @version 1.0
 * @datetime 2026/6/17 18:19
 */
@SpringBootApplication(scanBasePackages = {"com.ww.admin", "com.ww.cache", "com.ww.storage", "com.ww.common"})
@EntityScan(basePackages = {"com.ww.common", "com.ww.cache", "com.ww.admin"})
@EnableJpaRepositories(basePackages = {"com.ww.admin"})
@EnableJpaAuditing(auditorAwareRef = "auditorAware")
@ConfigurationPropertiesScan({"com.ww.admin", "com.ww.cache", "com.ww.storage", "com.ww.common"})
public class AdminApplication {
    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
    }
}
