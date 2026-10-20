package com.ww.runtime.server.lb;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ww.runtime.lb.LbConfig;
import com.ww.runtime.lb.LbConfigConflictException;
import com.ww.runtime.lb.LbConfigStore;

/**
 * 负载均衡配置管理端点（前缀 {@code /rt/lb}，走 default-deny 鉴权）。
 *
 * <ul>
 *   <li>{@code GET /rt/lb/config}：读取集群级 LB 配置（缺行返回默认配置）；</li>
 *   <li>{@code PUT /rt/lb/config}：整体更新，body.version 乐观锁；
 *       校验失败 400（{@link IllegalArgumentException} 经全局处理器），
 *       版本冲突 409；PUT 成功立即刷新本机缓存。</li>
 * </ul>
 */
@RestController
@RequestMapping("/rt/lb")
public class LbAdminController {

    private final LbConfigStore store;
    private final LbRefresher refresher;

    public LbAdminController(LbConfigStore store, LbRefresher refresher) {
        this.store = store;
        this.refresher = refresher;
    }

    @GetMapping("/config")
    public LbConfig getConfig() {
        return store.get();
    }

    @PutMapping("/config")
    public ResponseEntity<Object> updateConfig(@RequestBody LbConfig body) {
        try {
            LbConfig updated = store.update(body, body.version());
            refresher.refreshNow();
            return ResponseEntity.ok(updated);
        } catch (LbConfigConflictException conflict) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("message", conflict.getMessage(),
                            "actualVersion", conflict.actualVersion()));
        }
    }
}
