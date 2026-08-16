package com.ww.admin.controller;

import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ww.admin.service.log.ClusterLogStreamHub;

import jakarta.servlet.http.HttpServletResponse;

/**
 * 实时日志 SSE 控制器：建立服务端推送连接，把后台产生的日志实时推给前台。
 * <p>复用现有 Shiro 认证（请求头 Authorization）与 {@code log:*} 权限；可选按级别/节点/关键字过滤。
 * <p>集群模式下由 {@link ClusterLogStreamHub} 聚合本节点与所有对端节点的日志，
 * 任意节点接入均可收到全集群推送。
 */
@RestController
@RequestMapping("/log")
@RequiresPermissions("log:*")
public class LogStreamController {

    private final ClusterLogStreamHub clusterLogStreamHub;

    public LogStreamController(ClusterLogStreamHub clusterLogStreamHub) {
        this.clusterLogStreamHub = clusterLogStreamHub;
    }

    /**
     * 建立实时日志推送连接。
     * <p>响应头设置 SSE 相关优化：
     * <ul>
     *   <li>{@code X-Accel-Buffering: no}：通知 nginx 等反向代理关闭对该响应的缓冲，避免 SSE 帧被积压；</li>
     *   <li>{@code Cache-Control: no-cache}：禁止中间层缓存。</li>
     * </ul>
     *
     * @param level   级别过滤（可空）
     * @param node    节点过滤（可空）
     * @param keyword 关键字过滤（可空）
     */
    @GetMapping(value = "/stream", produces = "text/event-stream")
    public SseEmitter stream(@RequestParam(required = false) String level,
                             @RequestParam(required = false) String node,
                             @RequestParam(required = false) String keyword,
                             HttpServletResponse response) {
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        return clusterLogStreamHub.subscribe(level, node, keyword);
    }
}
