package com.ww.runtime.data.storage;

import com.ww.runtime.data.storage.engine.ObjectMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link StorageChunkCodec} 迁移分片编解码测试。
 */
@DisplayName("StorageChunkCodec 迁移分片编解码")
class StorageChunkCodecTest {

    @Test
    @DisplayName("T8：userMetadata 随分片编解码往返保持")
    void userMetadataRoundTrip() {
        ObjectMetadata meta = new ObjectMetadata("k1", 5, "text/plain", "etag", 100L, 3L);
        meta.setUserMetadata(Map.of("owner", "ww", "env", "prod"));
        byte[] chunk = StorageChunkCodec.encodeChunk("b1", meta,
                "hello".getBytes(StandardCharsets.UTF_8), 0, true);

        StorageChunkView view = StorageChunkCodec.decodeChunk(chunk);
        assertThat(view).isNotNull();
        assertThat(view.bucket()).isEqualTo("b1");
        assertThat(view.meta().getUserMetadata())
                .containsEntry("owner", "ww")
                .containsEntry("env", "prod");
        assertThat(new String(view.data(), StandardCharsets.UTF_8)).isEqualTo("hello");
        assertThat(view.fin()).isTrue();
    }

    @Test
    @DisplayName("T8：无 userMetadata 字段的旧格式分片解码容忍（空元数据）")
    void legacyChunkWithoutUserMetadataDecodesEmpty() {
        ObjectMetadata meta = new ObjectMetadata("k1", 5, "text/plain", "etag", 100L, 3L);
        // 未设置 userMetadata → 编码不写该字段，与存量旧分片同构
        byte[] chunk = StorageChunkCodec.encodeChunk("b1", meta,
                "hello".getBytes(StandardCharsets.UTF_8), 0, true);

        StorageChunkView view = StorageChunkCodec.decodeChunk(chunk);
        assertThat(view).isNotNull();
        assertThat(view.meta().getUserMetadata()).isEmpty();
    }
}
