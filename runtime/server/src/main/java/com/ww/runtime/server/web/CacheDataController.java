package com.ww.runtime.server.web;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.cache.CacheCapability;
import com.ww.runtime.data.cache.CacheGateway;

import tools.jackson.databind.ObjectMapper;

/**
 * CACHE 能力数据面端点（网关路由，REST 薄层，与节点无关）：
 * GET/PUT/DELETE {@code /cache/{key}} 单 key 读写删；
 * GET {@code /cache/keys} 集群级 key 列举（fan-out 聚合）；
 * DELETE {@code /cache/keys?prefix=} 子树（前缀）删除。
 *
 * <p>经 {@link CacheGateway} 按 placement 自动路由：分区属主在本机走本地引擎，
 * 否则经 gRPC 转发到属主节点（客户端无需感知拓扑）。写操作在属主端
 * {@link CacheCapability} 内带 placement epoch fencing；远端 fencing 拒绝经
 * stale-epoch 标记还原为 {@link StaleEpochException} → HTTP 409。
 *
 * <h2>值类型语义</h2>
 * PUT 携带 {@code type=json} 时以结构化对象（Map/List/基础类型）存储，
 * GET 响应 {@code type} 字段回显（Map/List → "json"，其余 → "text"）；
 * 默认 {@code type=text} 存原始字符串。
 */
@RestController
@RequestMapping("/cache")
public class CacheDataController {

    /** 管理面列举/子树删除的单次上限（防御大前缀拖死管理面）。 */
    private static final int MAX_KEYS_LIMIT = 10_000;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final CacheGateway cache;

    public CacheDataController(CacheGateway cache) {
        this.cache = cache;
    }

    /**
     * 集群级 key 列举（fan-out 聚合，与节点无关）：
     * {@code total} 为集群精确总数（countKeys），{@code truncated} 表示列举被 limit 截断。
     */
    @GetMapping("/keys")
    public ResponseEntity<Object> keys(@RequestParam(value = "prefix", required = false) String prefix,
                                       @RequestParam(value = "limit", defaultValue = "200") int limit) {
        int capped = Math.max(1, Math.min(limit, MAX_KEYS_LIMIT));
        String p = prefix == null || prefix.isEmpty() ? null : prefix;
        List<CacheCapability.CacheKeyEntry> entries = cache.listKeys(p, capped);
        long total = cache.countKeys(p);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total", total);
        body.put("keys", entries.stream().map(e -> Map.of(
                "key", e.key(),
                "ttlMillis", e.ttlMillis()
        )).toList());
        body.put("truncated", entries.size() < total);
        return ResponseEntity.ok(body);
    }

    /**
     * 子树删除：删除匹配前缀的全部 key（fan-out 列举后经网关<b>并行</b>路由删除，C28）。
     * 前缀下 key 超过上限时返回 400 要求细化前缀；个别 key 删除失败计数不回滚。
     * 注：列举与删除之间存在并发写窗口（TOCTOU），新写入的 key 不在本轮删除集，
     * 由调用方重试或细化前缀收敛。
     */
    @DeleteMapping("/keys")
    public ResponseEntity<Object> deleteByPrefix(@RequestParam(value = "prefix") String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "prefix must not be empty"));
        }
        long total = cache.countKeys(prefix);
        if (total > MAX_KEYS_LIMIT) {
            return ResponseEntity.badRequest().body(Map.of(
                    "message", "前缀下 key 超过 " + MAX_KEYS_LIMIT + "，请细化前缀"));
        }
        List<CacheCapability.CacheKeyEntry> entries = cache.listKeys(prefix, MAX_KEYS_LIMIT);
        long[] result = cache.deleteKeys(entries.stream()
                .map(CacheCapability.CacheKeyEntry::key).toList());
        return ResponseEntity.ok(Map.of("deleted", result[0], "failed", result[1]));
    }

    /** 读取（不做 fencing）：响应含 type 字段（Map/List → json，其余 → text）。 */
    @GetMapping("/{key}")
    public ResponseEntity<Object> get(@PathVariable("key") String key) {
        Object value = cache.get(key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("value", value);
        body.put("type", value instanceof Map || value instanceof List ? "json" : "text");
        return ResponseEntity.ok(body);
    }

    /**
     * 写入（带 fencing）：body 为值，ttl 查询参数为相对过期毫秒。
     * {@code type=json} 时解析为结构化对象存储（解析失败 → 400）；默认 text 存原始字符串。
     * {@code nx=true} 时仅当 key 不存在才写入：已存在返回 409（applied=false）。
     * 空 body（null 值）拒绝——null 值条目在管理面属异常状态（GET 无从区分「不存在」）。
     */
    @PutMapping("/{key}")
    public ResponseEntity<Object> put(@PathVariable("key") String key,
                                      @RequestParam(value = "ttl", defaultValue = "0") long ttlMillis,
                                      @RequestParam(value = "type", defaultValue = "text") String type,
                                      @RequestParam(value = "nx", defaultValue = "false") boolean nx,
                                      @RequestBody(required = false) String value) {
        if (value == null) {
            return ResponseEntity.badRequest()
                    .body(Map.of("message", "value 不能为空（如需删除请使用 DELETE）"));
        }
        Object toStore;
        if ("json".equals(type)) {
            try {
                toStore = JSON.readValue(value == null ? "" : value, Object.class);
            } catch (RuntimeException e) {
                return ResponseEntity.badRequest()
                        .body(Map.of("message", "invalid json: " + e.getMessage()));
            }
        } else {
            toStore = value;
        }
        if (nx) {
            boolean applied = cache.putIfAbsent(key, toStore, ttlMillis);
            if (!applied) {
                return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                        "key", key,
                        "ok", false,
                        "applied", false,
                        "message", "key already exists"));
            }
            return ResponseEntity.ok(Map.of("key", key, "ok", true, "applied", true));
        }
        cache.put(key, toStore, ttlMillis);
        return ResponseEntity.ok(Map.of("key", key, "ok", true));
    }

    /**
     * 删除（带 fencing，幂等）：key 不存在/已过期同样返回 204——
     * 目标状态（key 不存在）已达成；避免管理面把过期条目的删除当成错误。
     */
    @DeleteMapping("/{key}")
    public ResponseEntity<Object> delete(@PathVariable("key") String key) {
        cache.remove(key);
        return ResponseEntity.noContent().build();
    }

    /** fencing 拒绝（非属主/纪元过期）→ 409。 */
    @ExceptionHandler(StaleEpochException.class)
    public ResponseEntity<Object> staleEpoch(StaleEpochException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("message", e.getMessage()));
    }

    /** 冻结/未启动 → 423/503。 */
    @ExceptionHandler(RuntimeFrameworkException.class)
    public ResponseEntity<Object> framework(RuntimeFrameworkException e) {
        HttpStatus status = "frozen".equals(e.getMessage())
                ? HttpStatus.LOCKED : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status).body(Map.of("message", e.getMessage()));
    }
}
