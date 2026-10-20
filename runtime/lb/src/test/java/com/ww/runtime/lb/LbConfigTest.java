package com.ww.runtime.lb;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LbConfigTest {

    @Test
    void defaultConfigIsDisabledAndDataPlaneOnly() {
        LbConfig c = LbConfig.defaultConfig();
        assertThat(c.enabled()).isFalse();
        assertThat(c.entryNodeId()).isNull();
        assertThat(c.strategy()).isEqualTo(LbStrategy.ROUND_ROBIN);
        assertThat(c.selfServe()).isTrue();
        assertThat(c.pathPrefixes()).containsExactly("/cache/**", "/storage/**");
        assertThat(c.weightOverrides()).isEmpty();
        assertThat(c.version()).isZero();
    }

    @Test
    void enabledRequiresEntryNode() {
        assertThatThrownBy(() -> new LbConfig(true, "  ", LbStrategy.ROUND_ROBIN, true,
                List.of("/cache/**"), Map.of(), 1000, 1000, 1000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("entryNodeId");
    }

    @Test
    void protectedPrefixesRejected() {
        for (String bad : List.of("/", "/rt", "/rt/**", "/api/x", "/assets/a.js",
                "/index.html", "/favicon.ico")) {
            assertThatThrownBy(() -> new LbConfig(false, null, LbStrategy.ROUND_ROBIN, true,
                    List.of("/cache/**", bad), Map.of(), 1000, 1000, 1000, 1000, 0))
                    .as("prefix %s must be rejected", bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("protected");
        }
    }

    @Test
    void blankAndDuplicateAndRelativePrefixes() {
        assertThatThrownBy(() -> new LbConfig(false, null, LbStrategy.ROUND_ROBIN, true,
                List.of("cache/**"), Map.of(), 1000, 1000, 1000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class);

        LbConfig c = new LbConfig(false, null, LbStrategy.ROUND_ROBIN, true,
                List.of("/cache/**", "  ", "/cache/**", "/storage/**"),
                Map.of(), 1000, 1000, 1000, 1000, 0);
        assertThat(c.pathPrefixes()).containsExactly("/cache/**", "/storage/**");
    }

    @Test
    void weightOverridesMustBePositive() {
        assertThatThrownBy(() -> new LbConfig(false, null, LbStrategy.ROUND_ROBIN, true,
                List.of("/cache/**"), Map.of("node-2", 0.0), 1000, 1000, 1000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LbConfig(false, null, LbStrategy.ROUND_ROBIN, true,
                List.of("/cache/**"), Map.of("node-2", -1.0), 1000, 1000, 1000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void effectiveWeightPrefersOverride() {
        LbConfig c = new LbConfig(false, null, LbStrategy.WEIGHTED, true,
                List.of("/cache/**"), Map.of("node-2", 5.0), 1000, 1000, 1000, 1000, 0);
        assertThat(c.effectiveWeight("node-2", 1.0)).isEqualTo(5.0);
        assertThat(c.effectiveWeight("node-3", 2.5)).isEqualTo(2.5);
    }

    @Test
    void timingMustBePositive() {
        assertThatThrownBy(() -> new LbConfig(false, null, LbStrategy.ROUND_ROBIN, true,
                List.of("/cache/**"), Map.of(), 0, 1000, 1000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void withVersionKeepsContent() {
        LbConfig c = LbConfig.defaultConfig().withVersion(7);
        assertThat(c.version()).isEqualTo(7);
        assertThat(c.pathPrefixes()).isEqualTo(LbConfig.defaultConfig().pathPrefixes());
    }

    @Test
    void jsonRoundTrip() {
        tools.jackson.databind.ObjectMapper mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        LbConfig original = new LbConfig(true, "node-1", LbStrategy.WEIGHTED, false,
                List.of("/cache/**", "/storage/**"), Map.of("node-2", 3.0),
                2000, 500, 2000, 60000, 11);
        String json = mapper.writeValueAsString(original);
        LbConfig back = mapper.readValue(json, LbConfig.class);
        assertThat(back).isEqualTo(original);
    }
}
