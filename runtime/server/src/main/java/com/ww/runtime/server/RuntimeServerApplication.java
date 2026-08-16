package com.ww.runtime.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * runtime-server 节点进程入口：薄 Spring Boot 宿主。
 *
 * <p>核心模块（cluster / membership / capability / placement / scheduler / migration）
 * 均为 Spring-free POJO，由 {@code com.ww.runtime.server.config.RuntimeAssembly}
 * 构造注入装配；启动/停机顺序由
 * {@code com.ww.runtime.server.config.RuntimeBootstrapper}（SmartLifecycle）统一编排。</p>
 *
 * <p>配置前缀 {@code runtime.*}（见 {@link RuntimeProperties}），环境变量经 Spring
 * relaxed binding 覆盖（如 {@code RUNTIME_NODE_ID} → {@code runtime.node.id}）。</p>
 */
@SpringBootApplication(scanBasePackages = "com.ww.runtime.server")
@ConfigurationPropertiesScan("com.ww.runtime.server")
public class RuntimeServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(RuntimeServerApplication.class, args);
    }
}
