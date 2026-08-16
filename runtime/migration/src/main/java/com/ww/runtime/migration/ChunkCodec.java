package com.ww.runtime.migration;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 迁移数据分片编解码：分片 → checksum（SHA-256 hex）→ 校验。
 *
 * <p>与 proto 的 {@code DataChunkP} 字段一一对应的进程内载体是 {@link Chunk}；
 * 分片大小由调用方决定（传输默认 {@link #DEFAULT_CHUNK_SIZE}）。</p>
 */
public final class ChunkCodec {

    /** 默认分片大小：4MB。 */
    public static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;

    /** 条目内 key/value 分隔符（单字节 0x01）。 */
    public static final byte SEP_KV = 0x01;

    /** 条目终结符（单字节 0x02）。 */
    public static final byte SEP_ENTRY = 0x02;

    /**
     * DataChunkP 风格的分片载体。
     *
     * <p>注意：{@code data} 为可变数组且 record 的 equals/hashCode 按引用比较，
     * 比较内容请用 {@link java.util.Arrays#equals}。</p>
     *
     * @param taskId          归属任务（拉取场景无任务上下文时为 0）
     * @param seq             分片序号（从 0 起）
     * @param fin             是否最后一片
     * @param data            分片字节
     * @param checksum        SHA-256 hex
     * @param checkpointOffset 该分片发送完成后的累计进度偏移（codec 层按字节数累计；
     *                         条目级偏移由引擎/服务端按行格式另行累计）
     */
    public record Chunk(long taskId, int seq, boolean fin, byte[] data, String checksum,
                        long checkpointOffset) {
    }

    private ChunkCodec() {
    }

    /**
     * 将字节分片编码为带 checksum 的 Chunk 列表：seq 从 0 起，仅末片 fin=true，
     * checkpointOffset 按累计字节数记录。
     *
     * <p>空输入返回空列表（不产生 fin 片——「无数据」的 fin 语义由发送方自行表达）。</p>
     */
    public static List<Chunk> encode(long taskId, List<byte[]> pieces) {
        Objects.requireNonNull(pieces, "pieces");
        List<Chunk> chunks = new ArrayList<>(pieces.size());
        long cumulativeBytes = 0L;
        for (int i = 0; i < pieces.size(); i++) {
            byte[] data = Objects.requireNonNull(pieces.get(i), "piece");
            cumulativeBytes += data.length;
            chunks.add(new Chunk(taskId, i, i == pieces.size() - 1, data,
                    sha256Hex(data), cumulativeBytes));
        }
        return chunks;
    }

    /** SHA-256 摘要的十六进制小写表示。 */
    public static String sha256Hex(byte[] data) {
        Objects.requireNonNull(data, "data");
        MessageDigest digest = sha256();
        byte[] hash = digest.digest(data);
        StringBuilder hex = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16));
            hex.append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }

    /** 校验数据与 checksum（SHA-256 hex，大小写不敏感）是否一致。 */
    public static boolean verify(byte[] data, String checksum) {
        if (data == null || checksum == null || checksum.isBlank()) {
            return false;
        }
        return sha256Hex(data).equalsIgnoreCase(checksum.trim());
    }

    /** 统计一个分片内的条目数（按条目终结符 0x02 计数，行格式见 {@link MockPartitionAdapter}）。 */
    public static long countEntries(byte[] piece) {
        Objects.requireNonNull(piece, "piece");
        long count = 0L;
        for (byte b : piece) {
            if (b == SEP_ENTRY) {
                count++;
            }
        }
        return count;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
