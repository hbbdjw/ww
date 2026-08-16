package com.ww.runtime.server;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Duration;

import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
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
                .andExpect(jsonPath("$.members").exists())
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

    private static MockHttpServletRequestBuilder putDesired(String desired) {
        return put("/rt/nodes/node-1/capabilities/MOCK/desired")
                .contentType(APPLICATION_JSON)
                .content("{\"desired\":\"" + desired + "\"}");
    }
}
