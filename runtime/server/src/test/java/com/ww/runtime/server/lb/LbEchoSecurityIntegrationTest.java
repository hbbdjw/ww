package com.ww.runtime.server.lb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * LB 压测回显端点的装配/安全集成测试（memory 模式，不依赖 PostgreSQL 与任何能力）：
 * 无凭证 GET /cache/lb-echo 必须直达控制器（白名单），且回显本节点身份。
 */
@SpringBootTest(properties = {
        "runtime.store=memory",
        "runtime.node.id=node-it",
        "runtime.node.grpc-port=0",
        "server.port=0",
        "runtime.capabilities.mock-desired=false",
        "runtime.capabilities.cache-desired=false",
        "runtime.capabilities.storage-desired=false"
})
class LbEchoSecurityIntegrationTest {

    @Autowired
    private WebApplicationContext context;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity())
                .build();
    }

    @Test
    void echoIsReachableWithoutCredentialsAndReportsSelf() throws Exception {
        mvc.perform(get("/cache/lb-echo"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodeId").value("node-it"))
                .andExpect(jsonPath("$.forwarded").value(false))
                .andExpect(jsonPath("$.timestampMillis").isNumber());
    }
}
