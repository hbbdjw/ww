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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * CacheDataController NX 语义：首次写入 200/applied=true，已存在 409/applied=false；
 * 不带 nx 的 PUT 行为保持原样。
 */
class CacheDataControllerTest {

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
        capability = new CacheCapability(SELF, view, key -> 0, false, null);
        capability.start();
        gateway = new CacheGateway(SELF, view, key -> 0, capability, null, null);
        mvc = MockMvcBuilders.standaloneSetup(new CacheDataController(gateway)).build();
    }

    @AfterEach
    void tearDown() {
        gateway.close();
        capability.stop();
    }

    @Test
    void nxWritesOnceThenConflicts() throws Exception {
        mvc.perform(put("/cache/k")
                        .param("nx", "true")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("v1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.applied").value(true));

        mvc.perform(put("/cache/k")
                        .param("nx", "true")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("v2"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.applied").value(false))
                .andExpect(jsonPath("$.ok").value(false));

        mvc.perform(get("/cache/k"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("v1"));
    }

    @Test
    void encodedColonKeyRoundTrip() throws Exception {
        // URI 已是编码形态，直接传给 MockMvc，避免模板式构造二次编码
        java.net.URI path = java.net.URI.create("/cache/auth%3Asession%3Atok");

        mvc.perform(put(path)
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("v1"))
                .andExpect(status().isOk());

        mvc.perform(get(path))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.key").value("auth:session:tok"))
                .andExpect(jsonPath("$.value").value("v1"));

        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete(path))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteIsIdempotent() throws Exception {
        mvc.perform(delete("/cache/missing"))
                .andExpect(status().isNoContent());
    }

    @Test
    void plainPutStillOverwrites() throws Exception {
        mvc.perform(put("/cache/k")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("v1"))
                .andExpect(status().isOk());

        mvc.perform(put("/cache/k")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("v2"))
                .andExpect(status().isOk());

        mvc.perform(get("/cache/k"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("v2"));
    }
}
