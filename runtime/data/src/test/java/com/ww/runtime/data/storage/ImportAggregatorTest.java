package com.ww.runtime.data.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ImportAggregator} 迁移导入暂存聚合（T1）：
 * 流式追加/偏移连续性/三件套守护（并发上限/字节上限/超时清扫）/启动清扫。
 */
@DisplayName("ImportAggregator 导入暂存聚合（T1）")
class ImportAggregatorTest {

    @TempDir
    Path tempDir;

    private ImportAggregator aggregator(int maxAggregates, long maxBytes) {
        ImportAggregator agg = new ImportAggregator(
                tempDir.resolve("staging").toFile(), maxAggregates, maxBytes, 30);
        agg.start();
        return agg;
    }

    @Test
    @DisplayName("顺序追加 + finish：暂存文件字节与内容一致，暂存目录无残留")
    void appendThenFinishProducesStagedFile() throws IOException {
        try (ImportAggregator agg = aggregator(16, 1024)) {
            String key = "b1\0k1";
            assertThat(agg.append(key, "hello ".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            assertThat(agg.append(key, "world".getBytes(StandardCharsets.UTF_8), 6)).isTrue();

            File staged = agg.finish(key, 11L);
            assertThat(staged).isNotNull();
            assertThat(Files.readAllBytes(staged.toPath()))
                    .isEqualTo("hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(agg.pendingCount()).isZero();

            assertThat(staged.delete()).isTrue(); // 调用方消费后删除
            assertThat(agg.getStagingDir().listFiles((d, n) -> n.endsWith(".mig"))).isEmpty();
        }
    }

    @Test
    @DisplayName("偏移不连续：中止聚合并拒绝；完整重喂可恢复")
    void offsetDiscontinuityAbortsAndFullRefedRecovers() {
        try (ImportAggregator agg = aggregator(16, 1024)) {
            String key = "b1\0k2";
            // 跳过首片直接喂偏移 6 → 无既有聚合，拒绝
            assertThat(agg.append(key, "world".getBytes(StandardCharsets.UTF_8), 6)).isFalse();
            assertThat(agg.pendingCount()).isZero();

            // 完整重喂恢复
            assertThat(agg.append(key, "hello ".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            assertThat(agg.append(key, "world".getBytes(StandardCharsets.UTF_8), 6)).isTrue();
            assertThat(agg.finish(key, 11L)).isNotNull();
        }
    }

    @Test
    @DisplayName("既有聚合的乱序片：中止该聚合，重喂恢复")
    void outOfOrderOnExistingSessionAborts() {
        try (ImportAggregator agg = aggregator(16, 1024)) {
            String key = "b1\0k3";
            assertThat(agg.append(key, "aaa".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            // 乱序片（期望偏移 3，实喂 10）→ 中止
            assertThat(agg.append(key, "zzz".getBytes(StandardCharsets.UTF_8), 10)).isFalse();
            assertThat(agg.pendingCount()).isZero();

            // 重喂恢复
            assertThat(agg.append(key, "aaa".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            assertThat(agg.append(key, "bbb".getBytes(StandardCharsets.UTF_8), 3)).isTrue();
            assertThat(agg.finish(key, 6L)).isNotNull();
        }
    }

    @Test
    @DisplayName("字节上限（T12 联动）：超限中止 + 计数告警")
    void byteLimitExceededAborts() {
        try (ImportAggregator agg = aggregator(16, 10)) {
            String key = "b1\0k4";
            assertThat(agg.append(key, "12345".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            // 累计 5 + 6 > 10 → 中止
            assertThat(agg.append(key, "123456".getBytes(StandardCharsets.UTF_8), 5)).isFalse();
            assertThat(agg.getAbortedOnOverflow()).isEqualTo(1);
            assertThat(agg.pendingCount()).isZero();
        }
    }

    @Test
    @DisplayName("并发聚合上限：超限拒绝新建 + 计数告警")
    void concurrencyLimitRejects() {
        try (ImportAggregator agg = aggregator(2, 1024)) {
            assertThat(agg.append("b\0k-a", "x".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            assertThat(agg.append("b\0k-b", "x".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            // 第 3 个并发聚合被拒
            assertThat(agg.append("b\0k-c", "x".getBytes(StandardCharsets.UTF_8), 0)).isFalse();
            assertThat(agg.getRejectedOnConcurrency()).isEqualTo(1);
            // 完成一个后释放名额
            assertThat(agg.finish("b\0k-a", 1L)).isNotNull();
            assertThat(agg.append("b\0k-c", "x".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
        }
    }

    @Test
    @DisplayName("finish 尺寸不符：中止清理返回 null")
    void finishSizeMismatchReturnsNull() {
        try (ImportAggregator agg = aggregator(16, 1024)) {
            String key = "b1\0k5";
            assertThat(agg.append(key, "abc".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            assertThat(agg.finish(key, 99L)).isNull();
            assertThat(agg.pendingCount()).isZero();
            assertThat(agg.getStagingDir().listFiles((d, n) -> n.endsWith(".mig"))).isEmpty();
        }
    }

    @Test
    @DisplayName("超时清扫：超期未完成聚合被删除并计数")
    void sweepStaleRemovesExpiredSessions() throws Exception {
        try (ImportAggregator agg = aggregator(16, 1024)) {
            String key = "b1\0k6";
            assertThat(agg.append(key, "leak".getBytes(StandardCharsets.UTF_8), 0)).isTrue();

            Thread.sleep(30); // 制造活动间隔
            agg.sweepStale(1L); // 1ms 阈值：该聚合已超期

            assertThat(agg.getStaleSwept()).isEqualTo(1);
            assertThat(agg.pendingCount()).isZero();
            assertThat(agg.getStagingDir().listFiles((d, n) -> n.endsWith(".mig"))).isEmpty();
        }
    }

    @Test
    @DisplayName("启动清扫：重启残留暂存文件被删除")
    void startupSweepRemovesLeftoverFiles() throws IOException {
        File staging = tempDir.resolve("staging").toFile();
        assertThat(staging.mkdirs() || staging.isDirectory()).isTrue();
        File leftover = new File(staging, "deadbeef.mig");
        assertThat(leftover.createNewFile()).isTrue();

        try (ImportAggregator agg = new ImportAggregator(staging, 16, 1024, 30)) {
            agg.start();
            assertThat(agg.getStaleSwept()).isEqualTo(1);
            assertThat(leftover).doesNotExist();
        }
    }

    @Test
    @DisplayName("abort 幂等：重复中止不抛异常")
    void abortIsIdempotent() {
        try (ImportAggregator agg = aggregator(16, 1024)) {
            String key = "b1\0k7";
            assertThat(agg.append(key, "abc".getBytes(StandardCharsets.UTF_8), 0)).isTrue();
            agg.abort(key);
            agg.abort(key); // 二次中止安全
            assertThat(agg.pendingCount()).isZero();
        }
    }
}
