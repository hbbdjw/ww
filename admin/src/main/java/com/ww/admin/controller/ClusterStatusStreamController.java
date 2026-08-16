package com.ww.admin.controller;

import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ww.admin.service.ClusterStatusStreamHub;

import jakarta.servlet.http.HttpServletResponse;

/**
 * 集群状态实时推送（SSE）：前端订阅后周期接收集群聚合状态快照
 * （集群总览 + 缓存节点在线/指标 + 存储集群拓扑/健康/维护态），
 * 替代前端定时轮询，实现各类状态的实时动态刷新。
 *
 * <p>响应头设置 SSE 相关优化（与 LogStreamController 一致）：
 * <ul>
 *   <li>{@code X-Accel-Buffering: no}：通知 nginx 等反向代理关闭对该响应的缓冲；</li>
 *   <li>{@code Cache-Control: no-cache}：禁止中间层缓存。</li>
 * </ul>
 *
 * <p>认证走 Shiro dual 过滤器（Session/Token），与其它 /cluster/* 端点一致。
 */
@Controller
@RequestMapping("/cluster")
public class ClusterStatusStreamController {

    private final ClusterStatusStreamHub hub;

    public ClusterStatusStreamController(ClusterStatusStreamHub hub) {
        this.hub = hub;
    }

    @GetMapping(value = "/status/stream", produces = "text/event-stream")
    @ResponseBody
    public SseEmitter stream(HttpServletResponse response) {
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        return hub.subscribe();
    }
}
