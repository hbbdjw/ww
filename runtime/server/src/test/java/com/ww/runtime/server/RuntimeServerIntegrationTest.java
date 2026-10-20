package com.ww.runtime.server;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.http.MediaType.TEXT_PLAIN;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 * runtime-server 装配集成测试（runtime.store=memory，不依赖 PostgreSQL）。
 *
 * <p>gRPC 走随机端口（runtime.node.grpc-port=0）避免端口冲突；周期参数缩短以加速
 * 调度收敛。上下文启动即隐含验证：gRPC 绑定、membership 心跳线程、leader 租约
 * 选举与调谐循环全部就绪（RuntimeBootstrapper.start 任一步失败都会使上下文加载失败）；
 * 测试结束由 SmartLifecycle 自动倒序停机。</p>
 */
@SpringBootTest(properties = {
        "runtime.store=memory",
        "runtime.node.grpc-port=0",
        "server.port=0",
        "runtime.capabilities.mock-desired=false",
        "runtime.capabilities.cache-desired=true",
        "runtime.capabilities.storage-desired=true",
        "runtime.capabilities.storage-data-dir=./target/it-storage-data",
        "runtime.heartbeat-interval-ms=200",
        "runtime.scheduler.ttl-ms=5000",
        "runtime.scheduler.renew-interval-ms=500",
        "runtime.scheduler.reconcile-interval-ms=300",
        "runtime.partition-count=64",
})
class RuntimeServerIntegrationTest {

    @Autowired
    private WebApplicationContext context;

    private MockMvc mvc;

    @BeforeEach
    void setUp() throws Exception {
        // 显式应用 springSecurity()：MockMvc 才会挂载安全过滤链
        MockMvc bootstrap = MockMvcBuilders.webAppContextSetup(context)
                .apply(org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity())
                .build();
        String bearer = login(bootstrap);
        mvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity())
                .defaultRequest(get("/").header(HttpHeaders.AUTHORIZATION, "Bearer " + bearer))
                .build();
    }

    /** 登录 admin：503（会话缓存未就绪）时退避重试，其他状态必须 200。 */
    private static String login(MockMvc mvc) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000L;
        MvcResult result;
        while (true) {
            result = mvc.perform(post("/api/auth/login")
                            .contentType(APPLICATION_JSON)
                            .content("{\"username\":\"admin\",\"password\":\"admin123\"}"))
                    .andReturn();
            if (result.getResponse().getStatus() != 503) {
                break;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("login 503 (cache not ready) timed out");
            }
            Thread.sleep(250L);
        }
        org.junit.jupiter.api.Assertions.assertEquals(200, result.getResponse().getStatus(),
                "login failed: " + result.getResponse().getContentAsString());
        return com.jayway.jsonpath.JsonPath.read(result.getResponse().getContentAsString(), "$.token");
    }

    /** 装配完整无异常：gRPC 绑定 / 心跳线程 / leader 选举启动全部成功。 */
    @Test
    void contextLoads() {
    }

    @Test
    void statusReturnsNodeAndLeaderFields() throws Exception {
        mvc.perform(get("/rt/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodeId").value("node-1"))
                .andExpect(jsonPath("$.leader").exists())
                .andExpect(jsonPath("$.leaseOwner").exists())
                .andExpect(jsonPath("$.capabilities").exists())
                .andExpect(jsonPath("$.primariesByCapability").exists())
                .andExpect(jsonPath("$.self").exists())
                .andExpect(jsonPath("$.members").isArray())
                .andExpect(jsonPath("$.activeMigrationTasks").exists());
    }

    @Test
    void putDesiredActiveIsVisibleInStatusAndEventuallyConverged() throws Exception {
        // 写期望态（DesiredStateWriter 生效）
        mvc.perform(putDesired("ACTIVE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.desired").value("ACTIVE"));

        // 期望态立即可见（按 type 过滤，不依赖 capabilities 数组顺序：现含 CACHE/MOCK/STORAGE）
        mvc.perform(get("/rt/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.capabilities[?(@.type=='MOCK')].desired")
                        .value(org.hamcrest.Matchers.hasItem("ACTIVE")));

        // 调度收敛：短 reconcile 周期后 leader 拉起 MOCK，actual → ACTIVE
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> mvc.perform(get("/rt/status"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.capabilities[?(@.type=='MOCK')].actual")
                                .value(org.hamcrest.Matchers.hasItem("ACTIVE"))));
    }

    @Test
    void putDesiredInvalidValueReturns400() throws Exception {
        mvc.perform(putDesired("BOGUS"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").exists());
    }

    /** 缓存数据面：写入 → 读取 → key 列表可见 → 删除后 404（单节点即属主，fencing 通过）。 */
    @Test
    void cachePutGetListAndDeleteRoundTrip() throws Exception {
        // 等 CACHE 能力启动 + placement 就绪（keys 端点不再 503）
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> mvc.perform(get("/cache/keys"))
                        .andExpect(status().isOk()));

        mvc.perform(put("/cache/it-key").contentType(TEXT_PLAIN).content("it-value"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true));

        mvc.perform(get("/cache/it-key"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("it-value"));

        mvc.perform(get("/cache/keys"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").isNumber())
                .andExpect(jsonPath("$.truncated").value(false))
                .andExpect(jsonPath("$.keys[?(@.key=='it-key')]").exists());

        mvc.perform(delete("/cache/it-key"))
                .andExpect(status().isNoContent());

        mvc.perform(get("/cache/it-key"))
                .andExpect(status().isNotFound());
    }

    /** json 对象值（type=json）写入成功后必须在 keys 列表可见（管理面全值类型口径），且 GET 回显 type=json。 */
    @Test
    void cachePutJsonValueObjectVisibleInKeysList() throws Exception {
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> mvc.perform(get("/cache/keys"))
                        .andExpect(status().isOk()));

        // 记录写入前总数（缓存与会话条目共享同一数据面，按增量断言而非绝对值）
        MvcResult baseResult = mvc.perform(get("/cache/keys")).andReturn();
        long totalBefore = ((Number) com.jayway.jsonpath.JsonPath.read(
                baseResult.getResponse().getContentAsString(), "$.total")).longValue();

        mvc.perform(put("/cache/it-json-obj").contentType(APPLICATION_JSON)
                        .param("type", "json").content("{\"a\":1}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true));

        mvc.perform(get("/cache/keys"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.keys[?(@.key=='it-json-obj')]").exists())
                .andExpect(jsonPath("$.total").value((int) totalBefore + 1));

        mvc.perform(get("/cache/it-json-obj"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.type").value("json"))
                .andExpect(jsonPath("$.value.a").value(1));

        mvc.perform(delete("/cache/it-json-obj")).andExpect(status().isNoContent());
    }

    /** 空 body（null 值）写入拒绝 400——null 值条目在管理面属异常状态（GET 无从区分「不存在」）。 */
    @Test
    void cachePutEmptyBodyRejected() throws Exception {
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> mvc.perform(get("/cache/keys"))
                        .andExpect(status().isOk()));

        MvcResult baseResult = mvc.perform(get("/cache/keys")).andReturn();
        long totalBefore = ((Number) com.jayway.jsonpath.JsonPath.read(
                baseResult.getResponse().getContentAsString(), "$.total")).longValue();

        mvc.perform(put("/cache/it-empty").contentType(TEXT_PLAIN))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").exists());

        mvc.perform(get("/cache/keys"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value((int) totalBefore)); // 未产生不可见条目
    }

    /** 桶列表与统计契约：建桶 → 列桶含 name/createdAt → 上传对象 → stats=true 聚合 objectCount/totalSize → 递归删桶。 */
    @Test
    void storageBucketListWithStatsRoundTrip() throws Exception {
        String bucket = "it-stats-" + System.currentTimeMillis();

        // 等 STORAGE 能力 ACTIVE + 放置就绪
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> mvc.perform(get("/rt/status"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.capabilities[?(@.type=='STORAGE')].actual")
                                .value(org.hamcrest.Matchers.hasItem("ACTIVE"))));

        // 建桶（幂等）
        mvc.perform(put("/storage/" + bucket))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.created").value(true));

        // 列桶：默认只带 name/createdAt（元素为对象）
        mvc.perform(get("/storage"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.buckets[?(@.name=='" + bucket + "')].createdAt")
                        .value(org.hamcrest.Matchers.hasItem(org.hamcrest.Matchers.greaterThanOrEqualTo(0L))));

        // 上传两个对象（11 + 5 = 16 字节）
        mvc.perform(put("/storage/" + bucket + "/dir/a.txt").contentType(TEXT_PLAIN).content("hello world"))
                .andExpect(status().isOk());
        mvc.perform(put("/storage/" + bucket + "/b.log").contentType(TEXT_PLAIN).content("12345"))
                .andExpect(status().isOk());

        // stats=true：聚合 objectCount=2 / totalSize=16 / 非估算
        mvc.perform(get("/storage").param("stats", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.buckets[?(@.name=='" + bucket + "')].objectCount")
                        .value(org.hamcrest.Matchers.hasItem(2)))
                .andExpect(jsonPath("$.buckets[?(@.name=='" + bucket + "')].totalSize")
                        .value(org.hamcrest.Matchers.hasItem(16)))
                .andExpect(jsonPath("$.buckets[?(@.name=='" + bucket + "')].objectCountEstimate")
                        .value(org.hamcrest.Matchers.hasItem(false)));

        // 递归删桶后列表不再包含
        mvc.perform(delete("/storage/" + bucket).param("recursive", "true"))
                .andExpect(status().isNoContent());
        mvc.perform(get("/storage"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.buckets[?(@.name=='" + bucket + "')]").isEmpty());
    }

    /**
     * Range 请求契约（媒体预览边下边播）：HEAD 探测 Accept-Ranges: bytes；
     * 单段 Range → 206 + Content-Range + 精确窗口字节（含开区间与后缀形式）；
     * 越界 → 416 + bytes *&#47;N。流式响应经 asyncDispatch 取回写出内容。
     */
    @Test
    void storageObjectRangeReadServesPartialContent() throws Exception {
        String bucket = "it-range-" + System.currentTimeMillis();
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> mvc.perform(get("/rt/status"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.capabilities[?(@.type=='STORAGE')].actual")
                                .value(org.hamcrest.Matchers.hasItem("ACTIVE"))));

        mvc.perform(put("/storage/" + bucket)).andExpect(status().isOk());
        mvc.perform(put("/storage/" + bucket + "/media.bin")
                        .contentType("application/octet-stream")
                        .content("0123456789abcdef"))
                .andExpect(status().isOk());

        // HEAD：浏览器媒体元素以此探测定位能力
        mvc.perform(head("/storage/" + bucket + "/media.bin"))
                .andExpect(status().isOk())
                .andExpect(header().string(HttpHeaders.ACCEPT_RANGES, "bytes"))
                .andExpect(header().string(HttpHeaders.CONTENT_LENGTH, "16"));

        // 全量 GET 同样携带 Accept-Ranges
        MvcResult full = mvc.perform(get("/storage/" + bucket + "/media.bin"))
                .andExpect(request().asyncStarted())
                .andReturn();
        mvc.perform(asyncDispatch(full))
                .andExpect(status().isOk())
                .andExpect(header().string(HttpHeaders.ACCEPT_RANGES, "bytes"))
                .andExpect(content().bytes("0123456789abcdef".getBytes(StandardCharsets.UTF_8)));

        // bytes=4-9 → 206 窗口
        MvcResult mid = mvc.perform(get("/storage/" + bucket + "/media.bin")
                        .header(HttpHeaders.RANGE, "bytes=4-9"))
                .andExpect(request().asyncStarted())
                .andReturn();
        mvc.perform(asyncDispatch(mid))
                .andExpect(status().isPartialContent())
                .andExpect(header().string(HttpHeaders.CONTENT_RANGE, "bytes 4-9/16"))
                .andExpect(header().string(HttpHeaders.CONTENT_LENGTH, "6"))
                .andExpect(content().bytes("456789".getBytes(StandardCharsets.UTF_8)));

        // bytes=12- 开区间直到对象末尾
        MvcResult openEnded = mvc.perform(get("/storage/" + bucket + "/media.bin")
                        .header(HttpHeaders.RANGE, "bytes=12-"))
                .andExpect(request().asyncStarted())
                .andReturn();
        mvc.perform(asyncDispatch(openEnded))
                .andExpect(status().isPartialContent())
                .andExpect(header().string(HttpHeaders.CONTENT_RANGE, "bytes 12-15/16"))
                .andExpect(content().bytes("cdef".getBytes(StandardCharsets.UTF_8)));

        // bytes=-4 后缀：最后 4 字节
        MvcResult suffix = mvc.perform(get("/storage/" + bucket + "/media.bin")
                        .header(HttpHeaders.RANGE, "bytes=-4"))
                .andExpect(request().asyncStarted())
                .andReturn();
        mvc.perform(asyncDispatch(suffix))
                .andExpect(status().isPartialContent())
                .andExpect(header().string(HttpHeaders.CONTENT_RANGE, "bytes 12-15/16"))
                .andExpect(content().bytes("cdef".getBytes(StandardCharsets.UTF_8)));

        // 越界 → 416 + Content-Range: bytes */16（无流式体，同步响应）
        mvc.perform(get("/storage/" + bucket + "/media.bin")
                        .header(HttpHeaders.RANGE, "bytes=16-20"))
                .andExpect(status().isRequestedRangeNotSatisfiable())
                .andExpect(header().string(HttpHeaders.CONTENT_RANGE, "bytes */16"));

        mvc.perform(delete("/storage/" + bucket).param("recursive", "true"))
                .andExpect(status().isNoContent());
    }

    private static MockHttpServletRequestBuilder putDesired(String desired) {
        return put("/rt/nodes/node-1/capabilities/MOCK/desired")
                .contentType(APPLICATION_JSON)
                .content("{\"desired\":\"" + desired + "\"}");
    }

    // ------------------------------------------------------------------
    // 调度管理端点（RuntimeScheduleController）
    // ------------------------------------------------------------------

    @org.springframework.beans.factory.annotation.Autowired
    private com.ww.runtime.migration.MigrationTaskStore migrationTaskStore;

    @Test
    void migrationTasksListEmptyInitially() throws Exception {
        mvc.perform(get("/rt/migration/tasks"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(0))
                .andExpect(jsonPath("$.truncated").value(false))
                .andExpect(jsonPath("$.tasks").isEmpty());
    }

    /** 任务列表 + 取消流：直插 PENDING 任务 → 列表可见（DTO 扁平字段）→ 取消 → 幂等取消 → 未知 id 404。 */
    @Test
    void migrationTaskListAndCancelFlow() throws Exception {
        long taskId = migrationTaskStore.create("CACHE", 7,
                com.ww.runtime.common.NodeId.of("node-x"), com.ww.runtime.common.NodeId.of("node-1"));

        mvc.perform(get("/rt/migration/tasks").param("capability", "CACHE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.tasks[0].taskId").value((int) taskId))
                .andExpect(jsonPath("$.tasks[0].capability").value("CACHE"))
                .andExpect(jsonPath("$.tasks[0].partition").value(7))
                .andExpect(jsonPath("$.tasks[0].fromNode").value("node-x"))
                .andExpect(jsonPath("$.tasks[0].toNode").value("node-1"))
                .andExpect(jsonPath("$.tasks[0].state").value("PENDING"))
                .andExpect(jsonPath("$.tasks[0].sourceCount").value(-1));

        // scope=all 含终态任务；scope=active（默认）只含非终态
        mvc.perform(get("/rt/migration/tasks").param("scope", "all"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1));

        // 取消 → true；再取消（终态幂等）→ false
        mvc.perform(post("/rt/migration/tasks/{id}/cancel", taskId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cancelled").value(true));
        mvc.perform(post("/rt/migration/tasks/{id}/cancel", taskId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cancelled").value(false));

        // 取消后默认 scope 不再可见
        mvc.perform(get("/rt/migration/tasks"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(0));

        // 未知任务 → 404
        mvc.perform(post("/rt/migration/tasks/{id}/cancel", 999_999L))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").exists());
    }

    @Test
    void migrationResumeEndpoint() throws Exception {
        mvc.perform(post("/rt/migration/resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.resumed").value(true));
    }

    @Test
    void rebalanceResetReturnsLeaderInfo() throws Exception {
        mvc.perform(post("/rt/scheduler/rebalance")
                        .contentType(APPLICATION_JSON)
                        .content("{\"capability\":\"CACHE\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reset").value(true))
                .andExpect(jsonPath("$.isLeader").exists())
                .andExpect(jsonPath("$.leader").exists());

        // 非法能力 → 400
        mvc.perform(post("/rt/scheduler/rebalance")
                        .contentType(APPLICATION_JSON)
                        .content("{\"capability\":\"BOGUS\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").exists());
    }

    /** CACHE 引导后：distribution 的 primaries 总和 = partitionCount（测试配置 64）。 */
    @Test
    void placementDistributionMatchesPrimaries() throws Exception {
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> mvc.perform(get("/rt/placement/CACHE/distribution"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.capability").value("CACHE"))
                        .andExpect(jsonPath("$.partitionCount").value(64))
                        .andExpect(jsonPath("$.totalPlacements").value(64)) // cache rf=1 → 无副本行
                        .andExpect(jsonPath("$.distribution[0].nodeId").value("node-1"))
                        .andExpect(jsonPath("$.distribution[0].primaries").value(64)));

        // 非法能力 → 400
        mvc.perform(get("/rt/placement/BOGUS/distribution"))
                .andExpect(status().isBadRequest());
    }

    /** 每节点统计：单节点环境含 node-1 行（cache-desired=true → actual=ACTIVE）、keyCount 为数值。 */
    @Test
    void cacheNodeStatsIncludesSelf() throws Exception {
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> mvc.perform(get("/rt/cache/node-stats"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.nodes[?(@.nodeId=='node-1')].cacheActual")
                                .value(org.hamcrest.Matchers.hasItem("ACTIVE")))
                        .andExpect(jsonPath("$.nodes[?(@.nodeId=='node-1')].keyCount")
                                .value(org.hamcrest.Matchers.hasItem(
                                        org.hamcrest.Matchers.greaterThanOrEqualTo(0))))
                        .andExpect(jsonPath("$.nodes[?(@.nodeId=='node-1')].reachable")
                                .value(org.hamcrest.Matchers.hasItem(true))));
    }
}
