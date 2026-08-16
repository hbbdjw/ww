package com.ww.admin.controller;

import java.util.Map;

import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.admin.common.JsonResult;
import com.ww.admin.service.CacheAdminService;

/**
 * 缓存管理控制器：为管理后台提供类 Redis 的缓存浏览与运维 API（配合系统管理-缓存管理页面）。
 *
 * <p>端点设计（挂载于 /cache/admin）：
 * <ul>
 *   <li>POST /cache/admin/nodes  —— 缓存节点总览（在线状态 + 每节点统计）</li>
 *   <li>POST /cache/admin/keys   —— key 列表（按节点聚合，支持关键字过滤）</li>
 *   <li>POST /cache/admin/value  —— 读取 key 的值与剩余 TTL</li>
 *   <li>POST /cache/admin/save   —— 新增/修改 key-value（支持 TTL）</li>
 *   <li>POST /cache/admin/delete —— 删除 key</li>
 * </ul>
 *
 * <p>请求可能落在任意节点（前端经代理轮询），跨节点数据由 {@link CacheAdminService}
 * 经 /internal/cache/* fan-out 采集；写入/删除走分布式缓存 HRW 路由，天然幂等于节点选择。
 */
@RestController
@RequestMapping("/cache/admin")
@RequiresPermissions("cache:*")
public class CacheAdminController {

    private static final Logger log = LoggerFactory.getLogger(CacheAdminController.class);

    @Autowired
    private CacheAdminService cacheAdminService;

    // ==================== 节点总览 ====================

    /** 缓存节点总览：节点列表 + 在线状态 + 每节点本地统计（keyCount/CPU/堆内存） */
    @PostMapping("/nodes")
    public JsonResult nodes() {
        try {
            return JsonResult.success(cacheAdminService.nodesOverview());
        } catch (Exception e) {
            log.error("缓存节点总览失败", e);
            return JsonResult.error("缓存节点总览失败: " + e.getMessage());
        }
    }

    // ==================== Key 列表 ====================

    /**
     * key 列表：按节点聚合本地 key，附带持有节点。
     * <p>请求体：{"nodeKey": "host:port"(可选), "keyword": "过滤关键字"(可选), "limit": 2000(可选)}
     */
    @PostMapping("/keys")
    public JsonResult keys(@RequestBody(required = false) Map<String, Object> map) {
        String nodeKey = readString(map, "nodeKey");
        String keyword = readString(map, "keyword");
        int limit = 2000;
        if (map != null && map.get("limit") instanceof Number n) {
            limit = n.intValue();
        }
        try {
            return JsonResult.success(cacheAdminService.listKeys(nodeKey, keyword, limit));
        } catch (Exception e) {
            log.error("查询 key 列表失败: nodeKey={}, keyword={}", nodeKey, keyword, e);
            return JsonResult.error("查询 key 列表失败: " + e.getMessage());
        }
    }

    // ==================== Value 读取 ====================

    /**
     * 读取 key 的值与剩余 TTL。
     * <p>请求体：{"key": "...", "nodeKey": "host:port"(可选，优先在指定节点查找)}
     */
    @PostMapping("/value")
    public JsonResult value(@RequestBody Map<String, Object> map) {
        String key = readString(map, "key");
        if (key == null || key.isEmpty()) return JsonResult.warn("缺少参数 key");
        String nodeKey = readString(map, "nodeKey");
        try {
            return JsonResult.success(cacheAdminService.getValue(key, nodeKey));
        } catch (Exception e) {
            log.error("读取缓存值失败: key={}", key, e);
            return JsonResult.error("读取缓存值失败: " + e.getMessage());
        }
    }

    // ==================== 写入 / 删除 ====================

    /**
     * 新增/修改 key-value。
     * <p>请求体：{"key": "...", "value": "值（JSON 文本或普通字符串）", "ttlMillis": 0(可选，&lt;=0 永不过期)}
     */
    @PostMapping("/save")
    public JsonResult save(@RequestBody Map<String, Object> map) {
        String key = readString(map, "key");
        if (key == null || key.isEmpty()) return JsonResult.warn("缺少参数 key");
        Object value = map.get("value");
        String rawValue = value == null ? null : value.toString();
        long ttlMillis = 0;
        if (map.get("ttlMillis") instanceof Number n) {
            ttlMillis = n.longValue();
        }
        try {
            cacheAdminService.saveValue(key, rawValue, ttlMillis);
            return JsonResult.success(true, "保存成功");
        } catch (Exception e) {
            log.error("保存缓存值失败: key={}", key, e);
            return JsonResult.error("保存缓存值失败: " + e.getMessage());
        }
    }

    /** 删除 key。请求体：{"key": "..."} */
    @PostMapping("/delete")
    public JsonResult delete(@RequestBody Map<String, Object> map) {
        String key = readString(map, "key");
        if (key == null || key.isEmpty()) return JsonResult.warn("缺少参数 key");
        try {
            cacheAdminService.deleteKey(key);
            return JsonResult.success(true, "删除成功");
        } catch (Exception e) {
            log.error("删除缓存 key 失败: key={}", key, e);
            return JsonResult.error("删除缓存 key 失败: " + e.getMessage());
        }
    }

    // ==================== 内部工具 ====================

    private static String readString(Map<String, Object> map, String key) {
        if (map == null) return null;
        Object v = map.get(key);
        return v == null ? null : v.toString();
    }
}
