package com.ww.runtime.server.web;

import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.cache.CacheCapability;
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

import java.util.Map;

/**
 * CACHE 能力数据面端点（本地直连，首期 REST 薄层）：
 * GET/PUT/DELETE {@code /cache/{key}}。
 *
 * <p>写操作在 {@link CacheCapability} 内带 placement epoch fencing（非属主节点拒绝）；
 * 客户端先经 {@code /rt/placement/CACHE/lookup} 查询属主再直连本端点。
 * 高性能 TCP 协议数据面（复用 common/cache CacheProtocol）留待后续迭代。
 */
@RestController
@RequestMapping("/cache")
public class CacheDataController {

    private final CacheCapability cache;

    public CacheDataController(CacheCapability cache) {
        this.cache = cache;
    }

    /** 读取（不做 fencing）。 */
    @GetMapping("/{key}")
    public ResponseEntity<Object> get(@PathVariable("key") String key) {
        Object value = cache.get(key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(Map.of("key", key, "value", value));
    }

    /** 写入（带 fencing）：body 为值（字符串），ttl 查询参数为相对过期毫秒。 */
    @PutMapping("/{key}")
    public ResponseEntity<Object> put(@PathVariable("key") String key,
                                      @RequestParam(value = "ttl", defaultValue = "0") long ttlMillis,
                                      @RequestBody(required = false) String value) {
        cache.put(key, value, ttlMillis);
        return ResponseEntity.ok(Map.of("key", key, "ok", true));
    }

    /** 删除（带 fencing）。 */
    @DeleteMapping("/{key}")
    public ResponseEntity<Object> delete(@PathVariable("key") String key) {
        boolean removed = cache.remove(key);
        return removed ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
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
