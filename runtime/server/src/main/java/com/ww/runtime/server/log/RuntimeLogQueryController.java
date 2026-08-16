package com.ww.runtime.server.log;

import com.ww.runtime.log.LogRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 历史日志分页查询：{@code GET /rt/logs?level=&node=&keyword=&page=&size=}。
 *
 * <p>memory 模式（无 DataSource）返回 503——历史数据仅在 jdbc 模式落库。</p>
 */
@RestController
@RequestMapping("/rt/logs")
public class RuntimeLogQueryController {

    private final ObjectProvider<DataSource> dataSource;
    private volatile JdbcLogStore logStore;

    public RuntimeLogQueryController(ObjectProvider<DataSource> dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 分页查询（按时间倒序）。
     *
     * @param level   级别过滤（可空）
     * @param node    节点过滤（可空）
     * @param keyword 关键字过滤（可空，四字段匹配）
     * @param page    页码（从 1 起，默认 1）
     * @param size    页大小（默认 50，上限 500）
     */
    @GetMapping
    public ResponseEntity<Object> query(@RequestParam(required = false) String level,
                                        @RequestParam(required = false) String node,
                                        @RequestParam(required = false) String keyword,
                                        @RequestParam(defaultValue = "1") int page,
                                        @RequestParam(defaultValue = "50") int size) {
        JdbcLogStore store = resolveStore();
        if (store == null) {
            return ResponseEntity.status(503)
                    .body(Map.of("message", "log persistence disabled (runtime.store=memory)"));
        }
        int safePage = Math.max(1, page);
        int safeSize = Math.max(1, Math.min(500, size));
        long total = store.count(level, node, keyword);
        List<LogRecord> logs = store.page(level, node, keyword, (safePage - 1) * safeSize, safeSize);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total", total);
        body.put("page", safePage);
        body.put("size", safeSize);
        body.put("logs", logs);
        return ResponseEntity.ok(body);
    }

    private JdbcLogStore resolveStore() {
        JdbcLogStore store = logStore;
        if (store == null) {
            DataSource ds = dataSource.getIfAvailable();
            if (ds == null) {
                return null;
            }
            store = new JdbcLogStore(ds);
            logStore = store;
        }
        return store;
    }
}
