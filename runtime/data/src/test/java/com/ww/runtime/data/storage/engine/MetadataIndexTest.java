package com.ww.runtime.data.storage.engine;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link MetadataIndex} 容量止血语义（T3 第一步）：
 * 容量满拒绝新增 + 就地更新不受拦截 + 删除/删桶计数回收 + 降级标记。
 */
@DisplayName("MetadataIndex 容量止血（T3）")
class MetadataIndexTest {

    @TempDir
    Path tempDir;

    private Drive drive() {
        return new Drive(0, tempDir.resolve("drive").toString());
    }

    private ObjectMetadata meta(String key, long version) {
        return new ObjectMetadata(key, 1L, "text/plain", "etag-" + key, 0L, version);
    }

    @Test
    @DisplayName("容量满后新增被拒并计数；就地更新与删除不受拦截")
    void capacityFullRejectsWithCounters() {
        MetadataIndex index = new MetadataIndex(2);
        Drive d = drive();

        assertThat(index.putGuarded(d, "b1", "k1", meta("k1", 1))).isTrue();
        assertThat(index.putGuarded(d, "b1", "k2", meta("k2", 1))).isTrue();
        // 容量满：新增被拒
        assertThat(index.putGuarded(d, "b1", "k3", meta("k3", 1))).isFalse();
        assertThat(index.getRejectedOnCapacity()).isEqualTo(1);
        assertThat(index.totalEntries()).isEqualTo(2);

        // 就地更新既有键不受拦截
        assertThat(index.putGuarded(d, "b1", "k1", meta("k1", 2))).isTrue();
        assertThat(index.totalEntries()).isEqualTo(2);

        // 删除回收容量后新增恢复
        index.remove(d, "b1", "k2");
        assertThat(index.totalEntries()).isEqualTo(1);
        assertThat(index.putGuarded(d, "b1", "k3", meta("k3", 1))).isTrue();
        assertThat(index.totalEntries()).isEqualTo(2);
    }

    @Test
    @DisplayName("mergeGuarded：新增受容量拦截，命中既有键不拦截且版本取高")
    void mergeGuardedRespectsCapacity() {
        MetadataIndex index = new MetadataIndex(1);
        Drive d = drive();

        assertThat(index.mergeGuarded(d, "b1", "k1", meta("k1", 1))).isTrue();
        // 容量满：新键被拒
        assertThat(index.mergeGuarded(d, "b1", "k2", meta("k2", 1))).isFalse();
        // 既有键合并（更高版本）不受拦截，计数不变
        assertThat(index.mergeGuarded(d, "b1", "k1", meta("k1", 5))).isTrue();
        assertThat(index.totalEntries()).isEqualTo(1);
        assertThat(index.peekIndex(d, "b1").get("k1").getVersion()).isEqualTo(5);
        // 低版本合并保留高版本（防回退）
        assertThat(index.mergeGuarded(d, "b1", "k1", meta("k1", 3))).isTrue();
        assertThat(index.peekIndex(d, "b1").get("k1").getVersion()).isEqualTo(5);
    }

    @Test
    @DisplayName("maxEntries<=0 表示不限容量")
    void unlimitedWhenMaxIsZero() {
        MetadataIndex index = new MetadataIndex(0);
        Drive d = drive();
        for (int i = 0; i < 100; i++) {
            assertThat(index.putGuarded(d, "b1", "k" + i, meta("k" + i, 1))).isTrue();
        }
        assertThat(index.totalEntries()).isEqualTo(100);
        assertThat(index.getRejectedOnCapacity()).isZero();
    }

    @Test
    @DisplayName("dropBucket 回收计数并清除降级标记；clear 全量复位")
    void dropBucketAndClearReset() {
        MetadataIndex index = new MetadataIndex(2);
        Drive d = drive();
        index.putGuarded(d, "b1", "k1", meta("k1", 1));
        index.putGuarded(d, "b1", "k2", meta("k2", 1));
        index.degradeBucket("b1");
        assertThat(index.isDegraded("b1")).isTrue();

        index.dropBucket("b1");
        assertThat(index.totalEntries()).isZero();
        assertThat(index.isDegraded("b1")).isFalse();
        assertThat(index.peekIndex(d, "b1")).isNull();

        index.putDirect(d, "b2", "kx", meta("kx", 1));
        index.clear();
        assertThat(index.totalEntries()).isZero();
        assertThat(index.bucketsOn(d)).isEmpty();
    }
}
