package com.ww.runtime.server.log;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * 实时日志 SSE 控制器：建立服务端推送连接，把本节点采集的日志实时推给前台。
 *
 * <p>集群聚合由 {@link RuntimeLogStreamHub} 完成：连接任意节点即可收到全集群日志
 * （本地 LogBus 订阅 + 对端节点 gRPC 日志流转发）。可选按级别/节点/关键字过滤。</p>
 *
 * <p>响应头设置 SSE 相关优化：
 * <ul>
 *   <li>{@code X-Accel-Buffering: no}：通知 nginx 等反向代理关闭对该响应的缓冲，避免 SSE 帧被积压；</li>
 *   <li>{@code Cache-Control: no-cache}：禁止中间层缓存。</li>
 * </ul>
 */
@RestController
@RequestMapping("/rt/log")
public class RuntimeLogStreamController {

    private final RuntimeLogStreamHub hub;

    public RuntimeLogStreamController(RuntimeLogStreamHub hub) {
        this.hub = hub;
    }

    /**
     * 建立实时日志推送连接（全集群聚合）。
     *
     * @param level   级别过滤（可空）
     * @param node    节点过滤（可空）
     * @param keyword 关键字过滤（可空，四字段匹配）
     */
    @GetMapping(value = "/stream", produces = "text/event-stream")
    public SseEmitter stream(@RequestParam(required = false) String level,
                             @RequestParam(required = false) String node,
                             @RequestParam(required = false) String keyword,
                             HttpServletResponse response) {
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        return hub.subscribe(level, node, keyword);
    }
}
