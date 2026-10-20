package com.ww.runtime.server.web;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.cache.CacheCommandException;
import com.ww.runtime.data.cache.CacheCommandRequest;
import com.ww.runtime.data.cache.CacheGateway;

/**
 * CACHE 统一命令端点（内部使用）：{@code POST /cache/exec} 执行 Hash/List/Set/ZSet/
 * Stream/Keyspace 命令；{@code GET /cache/stats} 返回集群统计；{@code GET /cache/scan}
 * 提供本地游标遍历。
 *
 * <p>数据面仍按 placement 路由到属主；本端点不新增外部协议，仅把已有能力统一暴露。</p>
 */
@RestController
@RequestMapping("/cache")
public class CacheCommandController {

    private final CacheGateway cache;

    public CacheCommandController(CacheGateway cache) {
        this.cache = cache;
    }

    public record CommandBody(String command, List<Object> args) {
    }

    @PostMapping("/exec")
    public ResponseEntity<Object> execute(@RequestBody CommandBody body) {
        CacheCommandRequest request = new CacheCommandRequest(
                body == null ? null : body.command(),
                body == null ? null : body.args());
        Object result = cache.executeCommand(request);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("ok", true);
        response.put("result", result);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return cache.clusterStats();
    }

    @GetMapping("/scan")
    public ResponseEntity<Object> scan(@RequestParam(value = "cursor", defaultValue = "") String cursor,
                                       @RequestParam(value = "prefix", required = false) String prefix,
                                       @RequestParam(value = "count", defaultValue = "100") int count) {
        List<Object> args = new ArrayList<>();
        args.add(cursor);
        if (prefix != null && !prefix.isEmpty()) {
            args.add("MATCH");
            args.add(prefix);
        }
        args.add("COUNT");
        args.add(count);
        Object result = cache.executeCommand(new CacheCommandRequest("SCAN", args));
        return ResponseEntity.ok(result);
    }

    @ExceptionHandler(CacheCommandException.class)
    public ResponseEntity<Object> commandError(CacheCommandException e) {
        HttpStatus status = CacheCommandException.CROSSSLOT.equals(e.code())
                ? HttpStatus.CONFLICT : HttpStatus.BAD_REQUEST;
        return ResponseEntity.status(status).body(Map.of(
                "ok", false,
                "code", e.code(),
                "message", e.getMessage() == null ? "" : e.getMessage()
        ));
    }

    @ExceptionHandler(StaleEpochException.class)
    public ResponseEntity<Object> staleEpoch(StaleEpochException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("ok", false, "code", "STALE_EPOCH", "message", e.getMessage()));
    }

    @ExceptionHandler(RuntimeFrameworkException.class)
    public ResponseEntity<Object> framework(RuntimeFrameworkException e) {
        HttpStatus status = "frozen".equals(e.getMessage())
                ? HttpStatus.LOCKED : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status)
                .body(Map.of("ok", false, "code", "UNAVAILABLE",
                        "message", e.getMessage() == null ? "" : e.getMessage()));
    }
}
