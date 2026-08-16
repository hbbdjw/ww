package com.ww.admin.controller;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.ww.admin.common.JsonResult;
import com.ww.admin.service.ClusterAdminService;
import com.ww.cache.ClusterMembership;
import com.ww.cache.DistributedCache;
import com.ww.cache.FailoverManager;
import com.ww.cache.RebalanceManager;
import com.ww.cache.SimpleCache;
import com.ww.common.CachePortProperties;
import com.ww.common.ClusterNode;
import com.ww.common.ClusterProperties;
import com.ww.common.RendezvousHash;

import tools.jackson.databind.ObjectMapper;

/**
 * {@link ClusterAdminController} 单元测试：通过反射注入可选依赖，直接调用 handler 方法
 * （不走 Spring MVC，避免启动完整上下文），验证管理面 API 的正确性与降级行为。
 */
@DisplayName("ClusterAdminController 管理面 API")
class ClusterAdminControllerTest {

    private ClusterAdminController controller;
    private SimpleCache<String, Object> localCache;
    private ClusterMembership membership;
    private DistributedCache distributedCache;
    private FailoverManager failoverManager;
    private RebalanceManager rebalanceManager;
    private ClusterAdminService clusterAdminService;

    private ClusterNode self;
    private ClusterNode other1;
    private ClusterNode other2;

    @BeforeEach
    void setUp() throws Exception {
        controller = new ClusterAdminController();
        localCache = new SimpleCache<>(null);

        // 3 节点集群：self + other1 + other2
        self = mkNode("127.0.0.1", 7001, true);
        other1 = mkNode("127.0.0.1", 7002, false);
        other2 = mkNode("127.0.0.1", 7003, false);
        List<ClusterNode> nodes = List.of(self, other1, other2);

        distributedCache = new DistributedCache(localCache, new RendezvousHash(nodes), 3, self);
        membership = new ClusterMembership(nodes, self);
        distributedCache.setMembership(membership);

        failoverManager = new FailoverManager(membership, null);
        distributedCache.setFailoverManager(failoverManager);

        rebalanceManager = new RebalanceManager(membership, distributedCache, localCache, null);
        distributedCache.setRebalanceManager(rebalanceManager);

        // 构造集群管理服务并注入控制器
        clusterAdminService = new ClusterAdminService();
        CachePortProperties cachePortProperties = new CachePortProperties();
        cachePortProperties.setPort(7001);
        ClusterProperties clusterProperties = new ClusterProperties();
        clusterProperties.setNodes(nodes);
        injectIntoService("membership", membership);
        injectIntoService("distributedCache", distributedCache);
        injectIntoService("failoverManager", failoverManager);
        injectIntoService("rebalanceManager", rebalanceManager);
        injectIntoService("cachePortProperties", cachePortProperties);
        injectIntoService("clusterProperties", clusterProperties);
        injectIntoService("objectMapper", new ObjectMapper());
        injectIntoService("serverPort", 8080);

        inject("membership", membership);
        inject("distributedCache", distributedCache);
        inject("failoverManager", failoverManager);
        inject("rebalanceManager", rebalanceManager);
        inject("clusterAdminService", clusterAdminService);
    }

    private static ClusterNode mkNode(String host, int port, boolean self) {
        ClusterNode n = new ClusterNode(host, port, 1.0);
        n.setSelf(self);
        return n;
    }

    /** 通过反射注入私有字段（模拟 Spring @Autowired(required=false) 行为） */
    private void inject(String fieldName, Object value) throws Exception {
        Field f = ClusterAdminController.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(controller, value);
    }

    /** 通过反射注入 ClusterAdminService 私有字段 */
    private void injectIntoService(String fieldName, Object value) throws Exception {
        Field f = ClusterAdminService.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(clusterAdminService, value);
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

    // ==================== 查询类 ====================

    @Test
    @DisplayName("/cluster/status：返回拓扑版本、节点列表、failover/rebalance 状态")
    void shouldReturnStatus() {
        JsonResult r = controller.status();
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d).containsKey("topologyVersion");
        assertThat(d).containsKey("aliveCount");
        assertThat(d).containsKey("nodes");
        assertThat(d).containsKey("failover");
        assertThat(d).containsKey("rebalance");
        assertThat(d.get("aliveCount")).isEqualTo(3);
        assertThat((List<?>) d.get("nodes")).hasSize(3);
    }

    @Test
    @DisplayName("/cluster/topology：返回当前节点列表")
    void shouldReturnTopology() {
        JsonResult r = controller.topology();
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d).containsKey("topologyVersion");
        assertThat((List<?>) d.get("nodes")).hasSize(3);
    }

    @Test
    @DisplayName("/cluster/status 在 membership=null 时降级为静态节点视图（success + membershipEnabled=false）")
    void shouldDegradeWhenMembershipNull() throws Exception {
        inject("membership", null);
        injectIntoService("membership", null);
        JsonResult r = controller.status();
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("membershipEnabled")).isEqualTo(Boolean.FALSE);
        // 退化为静态 cluster.nodes 视图，节点列表仍返回
        assertThat((List<?>) d.get("nodes")).hasSize(3);
    }

    // ==================== Failover ====================

    @Test
    @DisplayName("/cluster/failover：标记节点为 DEAD 并应用拓扑")
    void shouldManuallyFailover() {
        JsonResult r = controller.failover(body("nodeKey", other1.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("nodeKey")).isEqualTo(other1.nodeKey());
        assertThat(d.get("changed")).isEqualTo(Boolean.TRUE);

        // membership 中 other1 应为 DEAD
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.DEAD);
        // HRW 应已重建：剩余 2 个 ALIVE 节点
        assertThat(distributedCache.getHash().getNodes()).hasSize(2);
    }

    @Test
    @DisplayName("/cluster/failover 缺少 nodeKey 返回 warn")
    void shouldRejectFailoverMissingNodeKey() {
        JsonResult r = controller.failover(new HashMap<>());
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
        assertThat(r.getMsg()).contains("nodeKey");
    }

    @Test
    @DisplayName("/cluster/failover 不允许 failover 自身")
    void shouldRejectFailoverSelf() {
        JsonResult r = controller.failover(body("nodeKey", self.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
        assertThat(r.getMsg()).contains("failover 失败");
    }

    @Test
    @DisplayName("/cluster/cancelFailover：恢复节点 ALIVE")
    void shouldCancelFailover() {
        controller.failover(body("nodeKey", other1.nodeKey()));
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.DEAD);

        JsonResult r = controller.cancelFailover(body("nodeKey", other1.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.ALIVE);
    }

    @Test
    @DisplayName("/cluster/failover 在 failoverManager=null 时返回 warn")
    void shouldWarnWhenFailoverManagerNull() throws Exception {
        inject("failoverManager", null);
        JsonResult r = controller.failover(body("nodeKey", other1.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
    }

    // ==================== Rebalance ====================

    @Test
    @DisplayName("/cluster/rebalance：触发一次重平衡")
    void shouldTriggerRebalance() {
        // 在本地写一个不属于 self 的 key，rebalance 应迁出
        // 直接构造一个会落到 other1 的 key（多次试探）
        String foreignKey = null;
        for (int i = 0; i < 200; i++) {
            String k = "rebal-test-" + i;
            List<ClusterNode> owners = distributedCache.getHash().selectNodes(k, 3);
            if (!owners.get(0).equals(self)) {
                foreignKey = k;
                break;
            }
        }
        if (foreignKey != null) {
            localCache.put(foreignKey, "v");
        }
        JsonResult r = controller.rebalance(new HashMap<>());
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d).containsKey("migratedKeys");
        assertThat(d).containsKey("status");
    }

    @Test
    @DisplayName("/cluster/rebalance 在 rebalanceManager=null 时返回 warn")
    void shouldWarnWhenRebalanceManagerNull() throws Exception {
        inject("rebalanceManager", null);
        JsonResult r = controller.rebalance(new HashMap<>());
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
    }

    // ==================== 节点加入/退出 ====================

    @Test
    @DisplayName("/cluster/join：新增节点，topologyVersion 递增")
    void shouldJoinNode() {
        long v0 = membership.getTopologyVersion();
        JsonResult r = controller.join(body("host", "127.0.0.1", "port", 18888, "weight", 1.5));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("nodeKey")).isEqualTo("127.0.0.1:18888");
        assertThat(d.get("added")).isEqualTo(Boolean.TRUE);
        assertThat(d.get("topologyApplied")).isEqualTo(Boolean.TRUE);
        assertThat(membership.getTopologyVersion()).isGreaterThan(v0);
        assertThat(membership.getNode("127.0.0.1:18888")).isNotNull();
    }

    @Test
    @DisplayName("/cluster/join 缺少 host/port 返回 warn")
    void shouldRejectJoinMissingArgs() {
        assertThat(controller.join(new HashMap<>()).getType()).isEqualTo(JsonResult.Type.warn);
        assertThat(controller.join(body("host", "127.0.0.1")).getType()).isEqualTo(JsonResult.Type.warn);
    }

    @Test
    @DisplayName("/cluster/join 默认 weight=1.0")
    void shouldDefaultWeight() {
        JsonResult r = controller.join(body("host", "127.0.0.1", "port", 18889));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        ClusterNode n = membership.getNode("127.0.0.1:18889");
        assertThat(n.getWeight()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("/cluster/leave：标记节点为 DEAD")
    void shouldLeaveNode() {
        JsonResult r = controller.leave(body("host", other1.getHost(), "port", other1.getPort()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("removed")).isEqualTo(Boolean.TRUE);
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.DEAD);
    }

    @Test
    @DisplayName("/cluster/leave 不允许让自身退出")
    void shouldRejectLeaveSelf() {
        JsonResult r = controller.leave(body("host", self.getHost(), "port", self.getPort()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
    }

    // ==================== suspect/alive ====================

    @Test
    @DisplayName("/cluster/suspect：标记节点为 SUSPECT")
    void shouldMarkSuspect() {
        JsonResult r = controller.suspect(body("nodeKey", other1.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.SUSPECT);
    }

    @Test
    @DisplayName("/cluster/alive：恢复节点 ALIVE")
    void shouldMarkAlive() {
        controller.suspect(body("nodeKey", other1.nodeKey()));
        JsonResult r = controller.alive(body("nodeKey", other1.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.ALIVE);
    }

    @Test
    @DisplayName("/cluster/alive 联动清除手动 failover 标记，节点恢复可探测")
    void shouldClearManualFailoverOnAlive() {
        // 手动 failover 后 manualDead 标记会阻止自动探测恢复
        controller.failover(body("nodeKey", other1.nodeKey()));
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.DEAD);

        JsonResult r = controller.alive(body("nodeKey", other1.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("manualFailoverCancelled")).isEqualTo(Boolean.TRUE);
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.ALIVE);
    }

    // ==================== applyTopology ====================

    @Test
    @DisplayName("/cluster/applyTopology：显式重建 HRW")
    void shouldApplyTopology() {
        // 把 other1 标 DEAD 后调 applyTopology
        membership.markDead(other1.nodeKey());
        JsonResult r = controller.applyTopology(new HashMap<>());
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("aliveCount")).isEqualTo(2);
        assertThat(distributedCache.getHash().getNodes()).hasSize(2);
    }

    @Test
    @DisplayName("/cluster/applyTopology 在 distributedCache=null 时返回 warn")
    void shouldWarnWhenDistributedCacheNull() throws Exception {
        inject("distributedCache", null);
        JsonResult r = controller.applyTopology(new HashMap<>());
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
    }

    // ==================== 集群列表 / 在线 / 指标 ====================

    @Test
    @DisplayName("/cluster/nodes：返回节点列表、在线状态与指标")
    void shouldReturnNodesOverview() {
        JsonResult r = controller.nodes();
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d).containsKey("topologyVersion");
        assertThat(d).containsKey("selfNodeKey");
        List<?> nodes = (List<?>) d.get("nodes");
        assertThat(nodes).hasSize(3);
        Map<String, ?> selfView = null;
        for (Object o : nodes) {
            Map<String, ?> view = (Map<String, ?>) o;
            assertThat(view).containsKeys("nodeKey", "online", "latencyMs", "status", "metrics");
            if (self.nodeKey().equals(view.get("nodeKey"))) selfView = view;
        }
        // self 节点必须在线，且带本机指标
        assertThat(selfView).isNotNull();
        assertThat(selfView.get("online")).isEqualTo(Boolean.TRUE);
        assertThat(selfView.get("metrics")).isInstanceOf(Map.class);
    }

    @Test
    @DisplayName("/cluster/metrics：返回本节点 CPU/内存指标")
    void shouldReturnLocalMetrics() {
        JsonResult r = controller.metrics();
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d).containsKeys("cpu", "memory", "jvm", "cache");
        Map<String, ?> cpu = (Map<String, ?>) d.get("cpu");
        assertThat(cpu).containsKeys("systemCpuLoad", "processCpuLoad", "availableProcessors");
        Map<String, ?> memory = (Map<String, ?>) d.get("memory");
        assertThat(memory).containsKeys("heapUsedMb", "heapUsedPercent", "systemTotalMb");
    }

    @Test
    @DisplayName("/cluster/nodes、/cluster/metrics 在 service=null 时返回 warn")
    void shouldWarnWhenServiceNull() throws Exception {
        inject("clusterAdminService", null);
        assertThat(controller.nodes().getType()).isEqualTo(JsonResult.Type.warn);
        assertThat(controller.metrics().getType()).isEqualTo(JsonResult.Type.warn);
    }

    // ==================== master 角色转移 ====================

    @Test
    @DisplayName("/cluster/transferMaster：源节点 master 角色转移到存活节点")
    void shouldTransferMaster() {
        // 找一个由 other1 担任 primary 的 key 写入本地缓存
        String ownedKey = null;
        for (int i = 0; i < 500; i++) {
            String k = "master-owner-" + i;
            List<ClusterNode> owners = distributedCache.getHash().selectNodes(k, 3);
            if (!owners.isEmpty() && owners.get(0).equals(other1)) {
                ownedKey = k;
                break;
            }
        }
        assertThat(ownedKey).isNotNull();
        localCache.put(ownedKey, "v");

        JsonResult r = controller.transferMaster(body("sourceNodeKey", other1.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("sourceNodeKey")).isEqualTo(other1.nodeKey());
        assertThat(d.get("transferred")).isEqualTo(Boolean.TRUE);
        assertThat(d.get("aliveCount")).isEqualTo(2);
        assertThat(d.get("sourceKeysHeldLocally")).isEqualTo(1);

        // 源节点已 DEAD，HRW 重建为 2 节点；新 primary 分布中不再包含源节点
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.DEAD);
        assertThat(distributedCache.getHash().getNodes()).hasSize(2);
        Map<String, ?> distribution = (Map<String, ?>) d.get("newPrimaryDistribution");
        assertThat(distribution).doesNotContainKey(other1.nodeKey());
    }

    @Test
    @DisplayName("/cluster/transferMaster 指定目标节点并校验其接管")
    void shouldTransferMasterWithTarget() {
        JsonResult r = controller.transferMaster(body(
                "sourceNodeKey", other1.nodeKey(),
                "targetNodeKey", other2.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.success);
        Map<String, Object> d = data(r);
        assertThat(d.get("targetNodeKey")).isEqualTo(other2.nodeKey());
        assertThat(d.get("aliveCount")).isEqualTo(2);
        assertThat(membership.getNode(other1.nodeKey()).getStatus())
                .isEqualTo(ClusterNode.Status.DEAD);
    }

    @Test
    @DisplayName("/cluster/transferMaster 缺少 sourceNodeKey 返回 warn")
    void shouldRejectTransferMasterMissingSource() {
        JsonResult r = controller.transferMaster(new HashMap<>());
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
        assertThat(r.getMsg()).contains("sourceNodeKey");
    }

    @Test
    @DisplayName("/cluster/transferMaster 不允许转移自身节点")
    void shouldRejectTransferMasterSelf() {
        JsonResult r = controller.transferMaster(body("sourceNodeKey", self.nodeKey()));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
        assertThat(r.getMsg()).contains("自身");
    }

    @Test
    @DisplayName("/cluster/transferMaster 目标节点不在 membership 返回 warn")
    void shouldRejectTransferMasterUnknownTarget() {
        JsonResult r = controller.transferMaster(body(
                "sourceNodeKey", other1.nodeKey(),
                "targetNodeKey", "127.0.0.1:9999"));
        assertThat(r.getType()).isEqualTo(JsonResult.Type.warn);
        assertThat(r.getMsg()).contains("目标节点");
    }
}
