package com.ww.runtime.server.security;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.jayway.jsonpath.JsonPath;

import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 安全过滤链集成测试（runtime.store=memory）：未认证 401、登录、Bearer 访问、
 * 错误凭证、坏 token、登出失效，以及系统管理接口的按钮级权限与前置校验。
 *
 * <p>登录前等待 CACHE 能力就绪（会话写缓存），重试到非 503。</p>
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
class RuntimeSecurityIntegrationTest {

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
    void unauthenticatedAndBadTokenRejected() throws Exception {
        mvc.perform(get("/rt/status")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/auth/info").header("Authorization", "Bearer not-a-real-token"))
                .andExpect(status().isUnauthorized());
    }

    /** 缺失静态资源（favicon 类常态噪音）→ 404 而非 500，且不落未预期异常堆栈。 */
    @Test
    void missingStaticResourceReturns404Not500() throws Exception {
        mvc.perform(get("/favicon.ico"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message")
                        .value(org.hamcrest.Matchers.containsString("favicon.ico")));
    }

    /** 存活探针无需鉴权且不泄露集群细节（部署脚本与编排系统依赖此约定）。 */
    @Test
    void healthProbeIsUnauthenticatedAndLeaksNoClusterDetail() throws Exception {
        mvc.perform(get("/rt/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                // 响应体仅含 status，不含节点 id / 成员数等集群细节
                .andExpect(jsonPath("$.nodeId").doesNotExist())
                .andExpect(jsonPath("$.members").doesNotExist())
                .andExpect(jsonPath("$.leader").doesNotExist());
    }

    @Test
    void loginWrongPasswordRejected() throws Exception {
        mvc.perform(post("/api/auth/login")
                        .contentType(APPLICATION_JSON)
                        .content("{\"username\":\"admin\",\"password\":\"wrong\"}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.message").value("用户名或密码错误"));
    }

    @Test
    void loginThenInfoWithBearerToken() throws Exception {
        String validToken = login();
        mvc.perform(get("/api/auth/info").header("Authorization", "Bearer " + validToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("admin"))
                .andExpect(jsonPath("$.roles[0]").value("super_admin"))
                .andExpect(jsonPath("$.permissions").isArray())
                .andExpect(jsonPath("$.permissions[?(@=='system:user:add')]").exists());
    }

    /**
     * GET 支持 access_token 查询参数回退（浏览器媒体元素无法附加 Authorization 头）。
     */
    @Test
    void getWithQueryParameterTokenIsAuthenticated() throws Exception {
        String validToken = login();
        mvc.perform(get("/api/auth/info?access_token=" + validToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("admin"));
    }

    /**
     * 查询参数令牌仅对 GET/HEAD 幂等读取生效：写方法（POST/PUT/DELETE）只接受请求头，
     * 无头携带 ?access_token= 仍按未认证 401，且令牌不被消费（后续带头访问仍有效）。
     */
    @Test
    void queryParameterTokenRejectedForWriteMethods() throws Exception {
        String validToken = login();
        mvc.perform(post("/api/auth/logout?access_token=" + validToken))
                .andExpect(status().isUnauthorized());
        // 未被登出：带头访问仍然有效
        mvc.perform(get("/api/auth/info").header("Authorization", "Bearer " + validToken))
                .andExpect(status().isOk());
    }

    @Test
    void logoutInvalidatesToken() throws Exception {
        String validToken = login();
        mvc.perform(post("/api/auth/logout").header("Authorization", "Bearer " + validToken))
                .andExpect(status().isOk());
        mvc.perform(get("/api/auth/info").header("Authorization", "Bearer " + validToken))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void userManagementEndpointsAndGuards() throws Exception {
        String validToken = login();

        mvc.perform(get("/api/system/users"))
                .andExpect(status().isUnauthorized());

        MvcResult listResult = mvc.perform(get("/api/system/users")
                        .header("Authorization", "Bearer " + validToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andReturn();
        long adminId = readLong(listResult.getResponse().getContentAsString(),
                "$.items[0].userId");

        // 重复用户名拒绝
        mvc.perform(post("/api/system/users")
                        .header("Authorization", "Bearer " + validToken)
                        .contentType(APPLICATION_JSON)
                        .content("{\"username\":\"admin\",\"nickname\":\"管理员\"}"))
                .andExpect(status().isBadRequest());

        // 新建用户并分配超管角色
        MvcResult created = mvc.perform(post("/api/system/users")
                        .header("Authorization", "Bearer " + validToken)
                        .contentType(APPLICATION_JSON)
                        .content("{\"username\":\"zhangsan\",\"nickname\":\"张三\","
                                + "\"roleIds\":[" + roleId(validToken) + "]}"))
                .andExpect(status().isOk())
                .andReturn();
        long newUserId = readLong(created.getResponse().getContentAsString(), "$.id");

        // 新建用户密码为默认 123456，可用该用户登录
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                mvc.perform(post("/api/auth/login")
                                .contentType(APPLICATION_JSON)
                                .content("{\"username\":\"zhangsan\",\"password\":\"123456\"}"))
                        .andExpect(status().isOk()));

        // 重置密码
        mvc.perform(put("/api/system/users/" + newUserId + "/reset-password")
                        .header("Authorization", "Bearer " + validToken)
                        .contentType(APPLICATION_JSON)
                        .content("{\"password\":\"newpass123\"}"))
                .andExpect(status().isOk());

        // 内置 admin 不可删、自身不可删
        mvc.perform(delete("/api/system/users/" + adminId)
                        .header("Authorization", "Bearer " + validToken))
                .andExpect(status().isBadRequest());
        mvc.perform(delete("/api/system/users/" + newUserId)
                        .header("Authorization", "Bearer " + validToken))
                .andExpect(status().isOk());
    }

    @Test
    void deptAndRoleGuards() throws Exception {
        String validToken = login();

        // 删除仍有用户的根部门 → 400
        String deptsJson = mvc.perform(get("/api/system/depts")
                        .header("Authorization", "Bearer " + validToken))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        long rootDeptId = readLong(deptsJson, "$.items[0].deptId");
        mvc.perform(delete("/api/system/depts/" + rootDeptId)
                        .header("Authorization", "Bearer " + validToken))
                .andExpect(status().isBadRequest());

        // 内置超管角色不可删
        long superRoleId = roleId(validToken);
        mvc.perform(delete("/api/system/roles/" + superRoleId)
                        .header("Authorization", "Bearer " + validToken))
                .andExpect(status().isBadRequest());

        // 菜单树可查询（供角色授权）
        mvc.perform(get("/api/system/menus")
                        .header("Authorization", "Bearer " + validToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].menuName").value("系统管理"));

        // 新建普通角色 → 菜单授权 → 删除（无用户绑定）
        MvcResult roleResult = mvc.perform(post("/api/system/roles")
                        .header("Authorization", "Bearer " + validToken)
                        .contentType(APPLICATION_JSON)
                        .content("{\"roleName\":\"观察员\",\"roleKey\":\"viewer\"}"))
                .andExpect(status().isOk()).andReturn();
        long roleId = readLong(roleResult.getResponse().getContentAsString(), "$.id");

        mvc.perform(put("/api/system/roles/" + roleId + "/menus")
                        .header("Authorization", "Bearer " + validToken)
                        .contentType(APPLICATION_JSON)
                        .content("{\"menuIds\":[" + firstMenuId(validToken) + "]}"))
                .andExpect(status().isOk());

        mvc.perform(delete("/api/system/roles/" + roleId)
                        .header("Authorization", "Bearer " + validToken))
                .andExpect(status().isOk());
    }

    // ------------------------------------------------------------------
    // 工具
    // ------------------------------------------------------------------

    /** 登录 admin（重试等待 CACHE 能力就绪：503 时退避重试）。 */
    private String login() throws Exception {
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
        return JsonPath.read(result.getResponse().getContentAsString(), "$.token");
    }

    private long roleId(String bearer) throws Exception {
        MvcResult result = mvc.perform(get("/api/system/roles")
                        .header("Authorization", "Bearer " + bearer))
                .andExpect(status().isOk()).andReturn();
        return readLong(result.getResponse().getContentAsString(),
                "$.items[0].roleId");
    }

    private long firstMenuId(String bearer) throws Exception {
        MvcResult result = mvc.perform(get("/api/system/menus")
                        .header("Authorization", "Bearer " + bearer))
                .andExpect(status().isOk()).andReturn();
        return readLong(result.getResponse().getContentAsString(),
                "$.items[0].menuId");
    }

    /** 读取 JSON 数值路径为 long（JsonPath 默认把小整数映射为 Integer）。 */
    private static long readLong(String json, String path) {
        Number value = JsonPath.read(json, path);
        return value.longValue();
    }
}
