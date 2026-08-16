package com.ww.admin.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ww.admin.service.log.LogStreamService;
import com.ww.common.CachePortProperties;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * 实时日志节点间内部端点：供其他节点的聚合 hub（{@code ClusterLogStreamHub}）拉取本节点日志流。
 *
 * <p>挂载于 Shiro 放行的 /internal/** 路径，通过 {@code X-Cache-Token} 请求头
 * 携带 {@code cache.auth-token} 自行校验（为空不校验），与 cache/storage 内部端点一致。
 * <p>仅推送本节点日志，不递归 fan-out，避免环路。
 */
@RestController
@RequestMapping("/internal/log")
public class InternalLogController {

    @Autowired
    private LogStreamService logStreamService;

    @Autowired
    private CachePortProperties cachePortProperties;

    /** 本节点实时日志流（过滤条件就地生效，减少转发量）；响应头禁止代理缓冲，保证 SSE 帧实时到达 hub */
    @GetMapping(value = "/stream", produces = "text/event-stream")
    public Object stream(@RequestParam(required = false) String level,
                         @RequestParam(required = false) String node,
                         @RequestParam(required = false) String keyword,
                         HttpServletRequest request,
                         HttpServletResponse response) {
        if (!checkAuth(request)) {
            return ResponseEntity.status(401).body("内部认证失败");
        }
        response.setHeader("X-Accel-Buffering", "no");
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        return logStreamService.subscribe(level, node, keyword);
    }

    // ==================== 工具 ====================

    private boolean checkAuth(HttpServletRequest request) {
        String token = cachePortProperties.getAuthToken();
        if (token == null || token.isEmpty()) {
            return true;
        }
        return token.equals(request.getHeader("X-Cache-Token"));
    }
}
