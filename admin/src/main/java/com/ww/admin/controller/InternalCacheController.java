package com.ww.admin.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.admin.common.JsonResult;
import com.ww.admin.service.CacheAdminService;
import com.ww.common.CachePortProperties;

import jakarta.servlet.http.HttpServletRequest;

/**
 * 缓存管理节点间内部端点：供其他节点的管理面 fan-out 采集（本地统计/key 列表/值读取）。
 *
 * <p>挂载于 Shiro 放行的 /internal/** 路径，通过 {@code X-Cache-Token} 请求头
 * 携带 {@code cache.auth-token} 自行校验（为空不校验），与 storage 内部端点一致。
 */
@RestController
@RequestMapping("/internal/cache")
public class InternalCacheController {

    @Autowired
    private CacheAdminService cacheAdminService;

    @Autowired
    private CachePortProperties cachePortProperties;

    /** 本节点本地统计（keyCount/CPU/堆内存），fan-out 只读不递归 */
    @GetMapping("/localStats")
    public ResponseEntity<JsonResult> localStats(HttpServletRequest request) {
        if (!checkAuth(request)) return unauthorized();
        return ResponseEntity.ok(JsonResult.success(cacheAdminService.localStats()));
    }

    /** 本节点本地 key 列表（字典序） */
    @GetMapping("/localKeys")
    public ResponseEntity<JsonResult> localKeys(HttpServletRequest request) {
        if (!checkAuth(request)) return unauthorized();
        return ResponseEntity.ok(JsonResult.success(cacheAdminService.localKeys()));
    }

    /** 本节点本地读取 key 的值与剩余 TTL。请求体：{"key": "..."} */
    @PostMapping("/localValue")
    public ResponseEntity<JsonResult> localValue(@RequestBody Map<String, Object> map,
                                                 HttpServletRequest request) {
        if (!checkAuth(request)) return unauthorized();
        Object k = map == null ? null : map.get("key");
        if (!(k instanceof String key) || key.isEmpty()) {
            return ResponseEntity.badRequest().body(JsonResult.warn("缺少参数 key"));
        }
        return ResponseEntity.ok(JsonResult.success(cacheAdminService.localValue(key)));
    }

    // ==================== 工具 ====================

    private boolean checkAuth(HttpServletRequest request) {
        String token = cachePortProperties.getAuthToken();
        if (token == null || token.isEmpty()) return true;
        return token.equals(request.getHeader("X-Cache-Token"));
    }

    private static ResponseEntity<JsonResult> unauthorized() {
        return ResponseEntity.status(401).body(JsonResult.error("内部认证失败"));
    }
}
