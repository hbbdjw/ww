package com.ww.runtime.migration;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ChunkCodec：分片/fin/seq/checksum、verify 篡改检测、条目计数。
 */
class ChunkCodecTest {

    @Test
    void sha256HexMatchesKnownVector() {
        byte[] abc = "abc".getBytes(StandardCharsets.UTF_8);
        assertThat(ChunkCodec.sha256Hex(abc))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assertThat(ChunkCodec.sha256Hex(new byte[0]))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void encodeAssignsSeqFinAndChecksum() {
        byte[] p0 = "piece-0".getBytes(StandardCharsets.UTF_8);
        byte[] p1 = "piece-1".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = "piece-2".getBytes(StandardCharsets.UTF_8);

        List<ChunkCodec.Chunk> chunks = ChunkCodec.encode(7L, List.of(p0, p1, p2));

        assertThat(chunks).hasSize(3);
        for (int i = 0; i < chunks.size(); i++) {
            ChunkCodec.Chunk chunk = chunks.get(i);
            assertThat(chunk.taskId()).isEqualTo(7L);
            assertThat(chunk.seq()).isEqualTo(i);
            assertThat(chunk.checksum()).isEqualTo(ChunkCodec.sha256Hex(chunks.get(i).data()));
            assertThat(ChunkCodec.verify(chunk.data(), chunk.checksum())).isTrue();
        }
        assertThat(chunks.get(0).fin()).isFalse();
        assertThat(chunks.get(1).fin()).isFalse();
        assertThat(chunks.get(2).fin()).isTrue();
    }

    @Test
    void encodeTracksCumulativeByteOffset() {
        byte[] a = new byte[10];
        byte[] b = new byte[20];
        byte[] c = new byte[5];
        List<ChunkCodec.Chunk> chunks = ChunkCodec.encode(1L, List.of(a, b, c));
        assertThat(chunks.get(0).checkpointOffset()).isEqualTo(10L);
        assertThat(chunks.get(1).checkpointOffset()).isEqualTo(30L);
        assertThat(chunks.get(2).checkpointOffset()).isEqualTo(35L);
    }

    @Test
    void encodeEmptyPiecesYieldsNoChunks() {
        assertThat(ChunkCodec.encode(1L, List.of())).isEmpty();
    }

    @Test
    void encodeSinglePieceIsFin() {
        List<ChunkCodec.Chunk> chunks = ChunkCodec.encode(2L,
                List.of("only".getBytes(StandardCharsets.UTF_8)));
        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0).seq()).isZero();
        assertThat(chunks.get(0).fin()).isTrue();
    }

    @Test
    void encodeRejectsNullPieces() {
        assertThatThrownBy(() -> ChunkCodec.encode(1L, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ChunkCodec.encode(1L, Arrays.asList((byte[]) null)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void verifyDetectsTampering() {
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        String checksum = ChunkCodec.sha256Hex(data);

        assertThat(ChunkCodec.verify(data, checksum)).isTrue();
        assertThat(ChunkCodec.verify(data, checksum.toUpperCase())).isTrue(); // 大小写不敏感

        byte[] tampered = "hellO".getBytes(StandardCharsets.UTF_8);
        assertThat(ChunkCodec.verify(tampered, checksum)).isFalse(); // 数据篡改
        assertThat(ChunkCodec.verify(data, ChunkCodec.sha256Hex(tampered))).isFalse(); // checksum 换绑
        assertThat(ChunkCodec.verify(data, null)).isFalse();
        assertThat(ChunkCodec.verify(data, "  ")).isFalse();
        assertThat(ChunkCodec.verify(null, checksum)).isFalse();
    }

    @Test
    void countEntriesCountsTerminators() {
        // 行格式 key 0x01 value 0x02：两个条目
        byte[] chunk = new byte[]{'k', 0x01, 'v', 0x02, 'k', 0x01, 'v', 0x02};
        assertThat(ChunkCodec.countEntries(chunk)).isEqualTo(2L);
        assertThat(ChunkCodec.countEntries(new byte[0])).isZero();
    }
}
