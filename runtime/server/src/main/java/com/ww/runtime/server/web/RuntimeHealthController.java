package com.ww.runtime.server.web;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.server.config.RuntimeBootstrapper;

/**
 * 存活/就绪探测端点（{@code GET /rt/health}）。
 *
 * <p>用途：部署脚本与编排系统的健康检查。<b>刻意保持无需鉴权</b>——探针必须在
 * 无凭证情况下可用；因此响应体只含存活状态，不含节点 id、成员数、分区等任何
 * 集群细节（这些信息仍在需鉴权的 {@code /rt/status} 下）。</p>
 *
 * <p>就绪语义：{@link RuntimeBootstrapper} 完成全部启动编排（store 自检、gRPC 注册、
 * 成员心跳、能力注册、调度调谐）后 isRunning 才为 true，故 503 → 200 的翻转即
 * 「节点真正可服务」的信号，而非仅 Tomcat 端口已监听。</p>
 */
@RestController
@RequestMapping("/rt")
public class RuntimeHealthController {

    private final RuntimeBootstrapper bootstrapper;

    public RuntimeHealthController(RuntimeBootstrapper bootstrapper) {
        this.bootstrapper = bootstrapper;
    }

    /** 就绪返回 200 {@code {"status":"UP"}}；启动编排未完成返回 503 {@code {"status":"STARTING"}}。 */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        boolean ready = bootstrapper.isRunning();
        Map<String, Object> body = Map.of("status", ready ? "UP" : "STARTING");
        return ready ? ResponseEntity.ok(body) : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
