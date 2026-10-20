package com.ww.runtime.server.web;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.data.cache.CacheCapability;
import com.ww.runtime.data.cache.CacheGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Optional;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** CacheCommandController：统一命令、统计与扫描端点。 */
class CacheCommandControllerTest {

    private static final NodeId SELF = NodeId.of("node-1");

    private CacheCapability capability;
    private CacheGateway gateway;
    private MockMvc mvc;

    @BeforeEach
    void setUp() throws Exception {
        PlacementView view = new PlacementView() {
            @Override
            public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
                return Optional.of(new Owner(SELF, 3L));
            }
        };
        capability = new CacheCapability(SELF, view, key -> key.charAt(0) - 'a', false, null);
        capability.start();
        gateway = new CacheGateway(SELF, view, key -> key.charAt(0) - 'a', capability, null, null);
        mvc = MockMvcBuilders.standaloneSetup(new CacheCommandController(gateway)).build();
    }

    @AfterEach
    void tearDown() {
        gateway.close();
        capability.stop();
    }

    @Test
    void execContainerCommandsAndStats() throws Exception {
        mvc.perform(post("/cache/exec")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"command\":\"HSET\",\"args\":[\"a:h\",\"f\",\"v\"]}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ok").value(true))
                .andExpect(jsonPath("$.result").value(1));

        mvc.perform(post("/cache/exec")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"command\":\"HGET\",\"args\":[\"a:h\",\"f\"]}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value("v"));

        mvc.perform(get("/cache/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodes[0].nodeId").value("node-1"))
                .andExpect(jsonPath("$.nodes[0].stats.started").value(true));

        mvc.perform(get("/cache/scan").param("prefix", "a:").param("count", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.keys[0]").value("a:h"))
                .andExpect(jsonPath("$.done").value(true));
    }

    @Test
    void execErrorsMapToHttpStatus() throws Exception {
        mvc.perform(post("/cache/exec")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"command\":\"NOPE\",\"args\":[\"a:k\"]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("UNKNOWN_COMMAND"));

        mvc.perform(post("/cache/exec")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"command\":\"MSET\",\"args\":[\"a:k1\",\"v1\",\"b:k2\",\"v2\"]}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("CROSSSLOT"));
    }
}
