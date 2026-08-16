package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockCapabilityTest {

    private static final NodeId SELF = NodeId.of("node-1");
    private static final NodeId OTHER = NodeId.of("node-2");

    /** key 首字母决定分区：a*→0, b*→1, c*→2, d*→3。 */
    private static int partitioner(String key) {
        return key.charAt(0) - 'a';
    }

    private StubView view;
    private MockCapability capability;

    @BeforeEach
    void setUp() {
        view = new StubView();
        capability = new MockCapability(SELF, view, MockCapabilityTest::partitioner);
    }

    // ---- fencing ----

    @Test
    void putRejectsWhenPartitionHasNoOwner() {
        view.owner = Optional.empty();
        assertThatThrownBy(() -> capability.put("a1", "v", 5L))
                .isInstanceOf(StaleEpochException.class)
                .satisfies(e -> {
                    StaleEpochException see = (StaleEpochException) e;
                    assertThat(see.expectedEpoch()).isEqualTo(5L);
                    assertThat(see.actualEpoch()).isEqualTo(-1L);
                });
        assertThat(capability.totalPuts()).isZero();
        assertThat(capability.totalItems()).isZero();
    }

    @Test
    void putRejectsWhenOwnerIsAnotherNode() {
        view.owner = Optional.of(new PlacementView.Owner(OTHER, 5L));
        assertThatThrownBy(() -> capability.put("a1", "v", 5L))
                .isInstanceOf(StaleEpochException.class)
                .satisfies(e -> {
                    StaleEpochException see = (StaleEpochException) e;
                    assertThat(see.expectedEpoch()).isEqualTo(5L);
                    assertThat(see.actualEpoch()).isEqualTo(5L);
                });
        assertThat(capability.totalPuts()).isZero();
    }

    @Test
    void putRejectsStaleEpoch() {
        view.owner = Optional.of(new PlacementView.Owner(SELF, 7L));
        // 旧纪元写（expected=5 < owner=7）被拒
        assertThatThrownBy(() -> capability.put("a1", "v", 5L))
                .isInstanceOf(StaleEpochException.class)
                .satisfies(e -> {
                    StaleEpochException see = (StaleEpochException) e;
                    assertThat(see.expectedEpoch()).isEqualTo(5L);
                    assertThat(see.actualEpoch()).isEqualTo(7L);
                });
        // 纪元不匹配（expected 超前）同样被拒
        assertThatThrownBy(() -> capability.put("a1", "v", 8L))
                .isInstanceOf(StaleEpochException.class);
        assertThat(capability.totalPuts()).isZero();
    }

    @Test
    void putAcceptsWhenOwnerSelfAndEpochMatches() {
        view.owner = Optional.of(new PlacementView.Owner(SELF, 7L));
        capability.put("a1", "v1", 7L);
        capability.put("a2", "v2", 7L);
        capability.put("b1", "v3", 7L);

        assertThat(capability.get("a1")).isEqualTo("v1");
        assertThat(capability.get("a2")).isEqualTo("v2");
        assertThat(capability.get("b1")).isEqualTo("v3");
        assertThat(capability.get("missing")).isNull();
        assertThat(capability.totalPuts()).isEqualTo(3);
        assertThat(capability.totalItems()).isEqualTo(3L);
        assertThat(capability.partitionCounts())
                .isEqualTo(Map.of(0, 2L, 1, 1L));
        assertThat(capability.partitionCount(0)).isEqualTo(2L);
        assertThat(capability.partitionCount(1)).isEqualTo(1L);
        assertThat(capability.partitionCount(2)).isZero();
    }

    @Test
    void conveniencePutUsesCurrentOwnerEpoch() {
        view.owner = Optional.of(new PlacementView.Owner(SELF, 9L));
        capability.put("a1", "v");
        assertThat(capability.get("a1")).isEqualTo("v");
        assertThat(capability.totalPuts()).isEqualTo(1);

        // 属主切到其他节点：便捷写同样被拒
        view.owner = Optional.of(new PlacementView.Owner(OTHER, 10L));
        assertThatThrownBy(() -> capability.put("a2", "v"))
                .isInstanceOf(StaleEpochException.class);
        assertThat(capability.totalPuts()).isEqualTo(1);
    }

    @Test
    void frozenPutThrowsWhileGetStillWorks() {
        view.owner = Optional.of(new PlacementView.Owner(SELF, 7L));
        capability.put("a1", "v", 7L);
        capability.freeze(true);
        assertThatThrownBy(() -> capability.put("a2", "v2", 7L))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessage("frozen");
        assertThat(capability.get("a1")).isEqualTo("v");
        assertThat(capability.totalPuts()).isEqualTo(1);

        capability.freeze(false);
        capability.put("a2", "v2", 7L);
        assertThat(capability.totalPuts()).isEqualTo(2);
    }

    // ---- 迁移往返 ----

    @Test
    void exportImportClearPartitionRoundTripIsExact() {
        view.owner = Optional.of(new PlacementView.Owner(SELF, 7L));
        capability.put("a1", "v1", 7L);
        capability.put("a2", "v2", 7L);
        capability.put("b1", "v3", 7L);

        List<Map.Entry<String, String>> exported = capability.exportPartition(0);
        assertThat(exported).hasSize(2);
        assertThat(exported.stream().map(Map.Entry::getKey))
                .containsExactlyInAnyOrder("a1", "a2");

        // 目标端（同 partitioner）导入分区 0
        MockCapability target = new MockCapability(OTHER, view, MockCapabilityTest::partitioner);
        target.importPartition(0, exported);
        assertThat(target.partitionCount(0)).isEqualTo(2L);
        assertThat(target.totalItems()).isEqualTo(2L);
        assertThat(target.get("a1")).isEqualTo("v1");
        assertThat(target.get("a2")).isEqualTo("v2");
        // 迁入不计入 totalPuts
        assertThat(target.totalPuts()).isZero();

        // 源端确认后清除分区 0
        capability.clearPartition(0);
        assertThat(capability.partitionCount(0)).isZero();
        assertThat(capability.totalItems()).isEqualTo(1L);
        assertThat(capability.get("b1")).isEqualTo("v3");

        // 目标端再导出：与源数据精确一致（往返无损）
        List<Map.Entry<String, String>> reExported = target.exportPartition(0);
        assertThat(reExported).containsExactlyInAnyOrderElementsOf(exported);
    }

    @Test
    void importPartitionRejectsForeignKeys() {
        List<Map.Entry<String, String>> wrongPartition = List.of(Map.entry("b1", "v"));
        assertThatThrownBy(() -> capability.importPartition(0, wrongPartition))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("b1");
    }

    @Test
    void moduleLifecycleHooksBehave() {
        view.owner = Optional.of(new PlacementView.Owner(SELF, 7L));
        assertThat(capability.type()).isEqualTo(CapabilityType.MOCK);
        assertThat(capability.resources()).isEqualTo(CapabilityModule.ResourceDeclaration.empty());

        capability.put("a1", "v1", 7L);
        capability.drain(); // 维护态停新写
        assertThatThrownBy(() -> capability.put("a2", "v2", 7L))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessage("frozen");

        capability.stop(); // 清空数据
        assertThat(capability.totalItems()).isZero();

        capability.start(); // 可重入：清空后再启动
        capability.put("a1", "v1", 7L);
        assertThat(capability.get("a1")).isEqualTo("v1");
        capability.warm(); // no-op，不抛
    }

    /** 可变桩视图：测试中直接改 owner 字段。 */
    private static final class StubView implements PlacementView {
        volatile Optional<Owner> owner = Optional.empty();

        @Override
        public Optional<Owner> owner(CapabilityType cap, PartitionId partition) {
            return owner;
        }
    }
}
