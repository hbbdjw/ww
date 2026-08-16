package com.ww.runtime.data.cache;

import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CacheCapability：fencing 写入、生命周期钩子与分区级迁移 API。
 */
class CacheCapabilityTest {

    private static final NodeId SELF = NodeId.of("node-1");
    private static final NodeId OTHER = NodeId.of("node-2");

    /** key 首字母决定分区：a*→0, b*→1。 */
    private static int partitioner(String key) {
        return key.charAt(0) - 'a';
    }

    private StubView view;
    private CacheCapability capability;

    @BeforeEach
    void setUp() throws Exception {
        view = new StubView();
        capability = new CacheCapability(SELF, view, CacheCapabilityTest::partitioner, false, null);
        capability.start();
        view.owner = Optional.of(new PlacementView.Owner(SELF, 3L));
    }

    // ---- fencing ----

    @Test
    void typeIsCache() {
        assertThat(capability.type()).isEqualTo(CapabilityType.CACHE);
    }

    @Test
    void putRejectsWhenPartitionHasNoOwner() {
        view.owner = Optional.empty();
        assertThatThrownBy(() -> capability.put("a1", "v", 0L))
                .isInstanceOf(StaleEpochException.class);
        assertThat(capability.get("a1")).isNull();
    }

    @Test
    void putRejectsWhenOwnerIsAnotherNode() {
        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThatThrownBy(() -> capability.put("a1", "v", 0L))
                .isInstanceOf(StaleEpochException.class);
    }

    @Test
    void putAcceptsWhenOwnerIsSelf() {
        capability.put("a1", "v1", 0L);
        capability.put("a2", 42, 60_000L); // 带 TTL
        assertThat(capability.get("a1")).isEqualTo("v1");
        assertThat(capability.get("a2")).isEqualTo(42);
        assertThat(capability.totalItems()).isEqualTo(2L);
    }

    @Test
    void removeRespectsFencingAndDeletes() {
        capability.put("a1", "v1", 0L);
        assertThat(capability.remove("a1")).isTrue();
        assertThat(capability.get("a1")).isNull();

        view.owner = Optional.of(new PlacementView.Owner(OTHER, 3L));
        assertThatThrownBy(() -> capability.remove("b1"))
                .isInstanceOf(StaleEpochException.class);
    }

    @Test
    void drainAndFreezeRejectWritesButAllowReads() {
        capability.put("a1", "v1", 0L);
        capability.drain();
        assertThatThrownBy(() -> capability.put("a2", "v", 0L))
                .isInstanceOf(RuntimeFrameworkException.class)
                .hasMessage("frozen");
        assertThat(capability.get("a1")).isEqualTo("v1"); // 读不受冻结影响

        capability.freeze(false);
        capability.put("a2", "v2", 0L); // 解冻恢复写
        assertThat(capability.get("a2")).isEqualTo("v2");
    }

    @Test
    void stopClearsDataAndRestartIsReentrant() throws Exception {
        capability.put("a1", "v1", 0L);
        capability.stop();
        assertThat(capability.totalItems()).isZero();

        capability.start();
        capability.put("a1", "v2", 0L);
        assertThat(capability.get("a1")).isEqualTo("v2");
        capability.warm(); // 不抛
    }

    // ---- 迁移数据面 ----

    @Test
    void exportImportClearPartitionRoundTrip() {
        capability.put("a1", "v1", 0L);
        capability.put("a2", "v2", 0L);
        capability.put("b1", "v3", 0L); // 分区 1，不应被导出

        List<CachePartitionEntry> exported = capability.exportPartition(0);
        assertThat(exported).hasSize(2);
        assertThat(exported.get(0).key()).isEqualTo("a1"); // 字典序
        assertThat(exported.get(1).key()).isEqualTo("a2");
        assertThat(capability.partitionCount(0)).isEqualTo(2L);
        assertThat(capability.partitionCount(1)).isEqualTo(1L);

        // 导入到“另一节点”的同能力实例
        CacheCapability target = newTarget();
        target.importPartition(0, exported);
        assertThat(target.get("a1")).isEqualTo("v1");
        assertThat(target.get("a2")).isEqualTo("v2");
        assertThat(target.partitionCount(0)).isEqualTo(2L);

        // 导入键不属于该分区 → 拒绝
        assertThatThrownBy(() -> target.importPartition(0,
                List.of(new CachePartitionEntry("b1", 0L, "x"))))
                .isInstanceOf(IllegalArgumentException.class);

        // 源端清空分区
        capability.clearPartition(0);
        assertThat(capability.partitionCount(0)).isZero();
        assertThat(capability.get("b1")).isEqualTo("v3"); // 其他分区不受影响
    }

    @Test
    void exportPreservesTtl() {
        capability.put("a1", "v", 60_000L);
        List<CachePartitionEntry> exported = capability.exportPartition(0);
        assertThat(exported).hasSize(1);
        assertThat(exported.get(0).remainingTtlMillis()).isBetween(1L, 60_000L);

        capability.put("a2", "v", 0L); // 永不过期 → 归一为 <=0
        exported = capability.exportPartition(0);
        CachePartitionEntry noTtl = exported.stream()
                .filter(e -> e.key().equals("a2")).findFirst().orElseThrow();
        assertThat(noTtl.remainingTtlMillis()).isLessThanOrEqualTo(0L);
    }

    private CacheCapability newTarget() {
        try {
            CacheCapability target = new CacheCapability(
                    OTHER, (cap, partition) -> Optional.of(new PlacementView.Owner(OTHER, 3L)),
                    CacheCapabilityTest::partitioner, false, null);
            target.start();
            return target;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
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
