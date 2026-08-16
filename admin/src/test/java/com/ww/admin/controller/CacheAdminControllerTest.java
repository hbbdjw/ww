package com.ww.admin.controller;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import com.ww.admin.common.JsonResult;
import com.ww.admin.service.CacheAdminService;
import com.ww.cache.ClusterMembership;
import com.ww.cache.DistributedCache;
import com.ww.cache.SimpleCache;
import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.RendezvousHash;

import tools.jackson.databind.ObjectMapper;

/**
 * {@link CacheAdminController} + {@link InternalCacheController} 单元测试：
 * 通过反射注入依赖，直接调用 handler 方法（不走 Spring MVC），验证缓存管理 API。
 *
 * <p>拓扑设计：membership 含 self + 一个不可达对端（验证 fan-out 降级）；
 * DistributedCache 的 HRW 环仅含 self（replicas=1），保证写入/删除确定性落在本地。
 */
@DisplayName("CacheAdminController 缓存管理 API")
class CacheAdminControllerTest {

    private CacheAdminController controller;
    private InternalCacheController internalController;
    private CacheAdminService service;
    private SimpleCache<String, Object> localCache;
    private CachePortProperties cachePortProperties;

    private ClusterNode self;
    private ClusterNode unreachablePeer;

    @BeforeEach
    void setUp() throws Exception {
        controller = new CacheAdminController();
        internalController = new InternalCacheController();
        service = new CacheAdminService();
        localCache = new SimpleCache<>(null);

        self = mkNode("127.0.0.1", 7001, true);
        unreachablePeer = mkNode("127.0.0.1", 59999, false);
        List<ClusterNode> nodes = List.of(self, unreachablePeer);
        ClusterMembership membership = new ClusterMembership(nodes, self);

        // HRW 环仅含 self：所有 key 确定性路由到本地
        DistributedCache distributedCache =
                new DistributedCache(localCache, new RendezvousHash(List.of(self)), 1, self);

        cachePortProperties = new CachePortProperties();
        cachePortProperties.setPort(7001);

        injectIntoService("membership", membership);
        injectIntoService("distributedCache", distributedCache);
        injectIntoService("cache", distributedCache);
        injectIntoService("localSimpleCache", localCache);
        injectIntoService("cachePortProperties", cachePortProperties);
        injectIntoService("objectMapper", new ObjectMapper());
        injectIntoService("serverPort", 8080);

        injectIntoController("cacheAdminService", service);
        injectIntoInternal("cacheAdminService", service);
        injectIntoInternal("cachePortProperties", cachePortProperties);
    }

    private static ClusterNode mkNode(String host, int port, boolean self) {
        ClusterNode n = new ClusterNode(host, port, 1.0);
        n.setSelf(self);
        return n;
    }

    private void injectIntoService(String fieldName, Object value) throws Exception {
        Field f = CacheAdminService.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(service, value);
    }

    private void injectIntoController(String fieldName, Object value) throws Exception {
        Field f = CacheAdminController.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(controller, value);
    }

    private void injectIntoInternal(String fieldName, Object value) throws Exception {
        Field f = InternalCacheController.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(internalController, value);
    }

    private static Map<String, Object> body(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> data(JsonResult r) {
        Object d = r.getData();
        assertThat(d).isInstanceOf(Map.class);
        return (Map<String, Object>) d;
    }

    // ==================== /cache/admin/nodes ====================

    @Test
    @DisplayName("/cache/admin/nodes：self 在线带本地统计，不可达对端标记离线")
    @SuppressWarnings("unchecked")
    void nodesOverviewShouldMarkPeerOffline() {
        localCache.put("k1", "v1");
        JsonResult r = controller.nodes();
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("totalKeyCount")).isEqualTo(1L);
        assertThat(d.get("onlineCount")).isEqualTo(1);
        List<?> nodes = (List<?>) d.get("nodes");
        assertThat(nodes).hasSize(2);
        for (Object o : nodes) {
            @SuppressWarnings("unchecked")
            Map<String, Object> view = (Map<String, Object>) o;
            if (self.nodeKey().equals(view.get("nodeKey"))) {
                assertThat(view.get("online")).isEqualTo(Boolean.TRUE);
                assertThat(view.get("self")).isEqualTo(Boolean.TRUE);
                Map<String, Object> stats = (Map<String, Object>) view.get("stats");
                assertThat(stats).containsKeys("keyCount", "heapUsedMb", "processCpuLoad");
                assertThat(stats.get("keyCount")).isEqualTo(1);
            } else {
                assertThat(view.get("online")).isEqualTo(Boolean.FALSE);
            }
        }
    }

    // ==================== /cache/admin/keys ====================

    @Test
    @DisplayName("/cache/admin/keys：聚合本地 key 并支持关键字过滤与节点标注")
    @SuppressWarnings("unchecked")
    void keysShouldAggregateAndFilter() {
        localCache.put("user:1", "a");
        localCache.put("user:2", "b");
        localCache.put("order:9", "c");

        // 全量：不可达对端不贡献数据，仅 self 3 条
        Map<String, Object> all = data(controller.keys(body()));
        assertThat(all.get("total")).isEqualTo(3);
        List<Map<String, Object>> entries = (List<Map<String, Object>>) all.get("keys");
        assertThat(entries).hasSize(3);
        assertThat(entries).allSatisfy(e -> assertThat(e.get("nodeKey")).isEqualTo(self.nodeKey()));

        // 关键字过滤（大小写不敏感）
        Map<String, Object> filtered = data(controller.keys(body("keyword", "USER")));
        assertThat(filtered.get("total")).isEqualTo(2);

        // 指定节点
        Map<String, Object> byNode = data(controller.keys(body("nodeKey", self.nodeKey())));
        assertThat(byNode.get("total")).isEqualTo(3);

        // 未知节点
        Map<String, Object> unknown = data(controller.keys(body("nodeKey", "1.2.3.4:1")));
        assertThat(unknown.get("total")).isEqualTo(0);
        assertThat(unknown).containsKey("error");
    }

    // ==================== /cache/admin/value ====================

    @Test
    @DisplayName("/cache/admin/value：读取存在的 key 返回值与 TTL，缺失返回 exists=false")
    @SuppressWarnings("unchecked")
    void valueShouldReadLocalKey() {
        localCache.put("user:1", Map.of("name", "tom"));

        Map<String, Object> hit = data(controller.value(body("key", "user:1")));
        assertThat(hit.get("exists")).isEqualTo(Boolean.TRUE);
        assertThat(hit.get("nodeKey")).isEqualTo(self.nodeKey());
        assertThat(hit.get("ttlMillis")).isEqualTo(Long.MAX_VALUE);
        assertThat((Map<String, Object>) hit.get("value")).containsEntry("name", "tom");

        Map<String, Object> miss = data(controller.value(body("key", "no:such:key")));
        assertThat(miss.get("exists")).isEqualTo(Boolean.FALSE);

        // 缺少参数
        assertThat(controller.value(body()).getType()).isEqualTo(JsonResult.Type.warn);
    }

    // ==================== /cache/admin/save ====================

    @Test
    @DisplayName("/cache/admin/save：JSON 文本解析为结构，普通文本保留字符串，TTL 生效")
    @SuppressWarnings("unchecked")
    void saveShouldParseJsonAndApplyTtl() {
        // JSON 对象
        assertThat(controller.save(body("key", "cfg:obj", "value", "{\"a\":1,\"b\":\"x\"}"))
                .getType()).isEqualTo(JsonResult.Type.success);
        Object objVal = localCache.get("cfg:obj");
        assertThat(objVal).isInstanceOf(Map.class);
        assertThat((Map<String, Object>) objVal).containsEntry("b", "x");

        // 普通字符串
        assertThat(controller.save(body("key", "cfg:str", "value", "hello world"))
                .getType()).isEqualTo(JsonResult.Type.success);
        assertThat(localCache.get("cfg:str")).isEqualTo("hello world");

        // TTL：保存后剩余 TTL 应落在 (0, 60000] 区间
        assertThat(controller.save(body("key", "cfg:ttl", "value", "temp", "ttlMillis", 60_000))
                .getType()).isEqualTo(JsonResult.Type.success);
        long ttl = localCache.remainingTtlMillis("cfg:ttl");
        assertThat(ttl).isBetween(1L, 60_000L);

        // 缺少参数
        assertThat(controller.save(body("value", "x")).getType()).isEqualTo(JsonResult.Type.warn);
    }

    // ==================== /cache/admin/delete ====================

    @Test
    @DisplayName("/cache/admin/delete：删除存在的 key")
    void deleteShouldRemoveKey() {
        localCache.put("tmp:key", "v");
        assertThat(controller.delete(body("key", "tmp:key")).getType())
                .isEqualTo(JsonResult.Type.success);
        assertThat(localCache.get("tmp:key")).isNull();

        assertThat(controller.delete(body()).getType()).isEqualTo(JsonResult.Type.warn);
    }

    // ==================== /internal/cache/* ====================

    @Test
    @DisplayName("/internal/cache：无 token 配置时放行，返回本地统计/key/值")
    @SuppressWarnings("unchecked")
    void internalEndpointsShouldWorkWithoutToken() {
        localCache.put("user:1", "a");

        ResponseEntity<JsonResult> stats =
                internalController.localStats(new MockHttpServletRequest());
        assertThat(stats.getStatusCode().value()).isEqualTo(200);
        assertThat(data(stats.getBody()).get("keyCount")).isEqualTo(1);

        ResponseEntity<JsonResult> keys =
                internalController.localKeys(new MockHttpServletRequest());
        assertThat(keys.getStatusCode().value()).isEqualTo(200);
        assertThat((List<String>) keys.getBody().getData()).contains("user:1");

        ResponseEntity<JsonResult> value = internalController.localValue(
                body("key", "user:1"), new MockHttpServletRequest());
        assertThat(value.getStatusCode().value()).isEqualTo(200);
        Map<String, Object> v = data(value.getBody());
        assertThat(v.get("exists")).isEqualTo(Boolean.TRUE);
        assertThat(v.get("value")).isEqualTo("a");
    }

    @Test
    @DisplayName("/internal/cache：配置 token 后校验 X-Cache-Token")
    void internalEndpointsShouldEnforceToken() {
        cachePortProperties.setAuthToken("secret");

        MockHttpServletRequest noToken = new MockHttpServletRequest();
        assertThat(internalController.localStats(noToken).getStatusCode().value()).isEqualTo(401);

        MockHttpServletRequest wrongToken = new MockHttpServletRequest();
        wrongToken.addHeader("X-Cache-Token", "bad");
        assertThat(internalController.localKeys(wrongToken).getStatusCode().value()).isEqualTo(401);

        MockHttpServletRequest okToken = new MockHttpServletRequest();
        okToken.addHeader("X-Cache-Token", "secret");
        assertThat(internalController.localStats(okToken).getStatusCode().value()).isEqualTo(200);
    }
}
