package com.ww.runtime.data.storage;

import com.ww.runtime.migration.ChunkCodec;
import com.ww.storage.ObjectMetadata;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * STORAGE 能力迁移分片编解码。
 *
 * <p>分片格式（UTF-8）：{@code headerJson + 0x01 + base64(dataSlice) [+ 0x02 仅 fin 片]}。
 * 单对象按 {@code chunkSize} 切为多片；<b>仅末片（fin）带终结符 0x02</b>，
 * 使 {@link ChunkCodec#countEntries} 的条目计数等于「已完成对象数」——
 * 断点续跑的 checkpoint 因此始终对齐对象边界，恢复后导出自完整对象起、
 * 导入自对象首片起，聚合不会残缺。
 *
 * <p>base64 保证数据片不含 0x01/0x02；header 为 JSON（控制字符自动转义）。
 */
public final class StorageChunkCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StorageChunkCodec() {
    }

    /**
     * 编码一个对象数据片。
     *
     * @param meta        对象元数据（随每片携带，import 端无需跨片状态即可校验）
     * @param data        本片数据（<= chunkSize）
     * @param offset      本片在对象内的字节偏移
     * @param fin         是否对象最后一片
     */
    public static byte[] encodeChunk(String bucket, ObjectMetadata meta, byte[] data,
                                     long offset, boolean fin) {
        ObjectNode header = MAPPER.createObjectNode();
        header.put("bucket", bucket);
        header.put("key", meta.getKey());
        header.put("size", meta.getSize());
        header.put("version", meta.getVersion());
        header.put("etag", meta.getEtag() == null ? "" : meta.getEtag());
        header.put("contentType", meta.getContentType());
        header.put("createdAt", meta.getCreatedAt());
        header.put("offset", offset);
        header.put("fin", fin);
        String line = header.toString()
                + Character.toString(ChunkCodec.SEP_KV)
                + Base64.getEncoder().encodeToString(data)
                + (fin ? Character.toString(ChunkCodec.SEP_ENTRY) : "");
        return line.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 反解分片；格式非法返回 null（调用方判 false）。
     */
    public static StorageChunkView decodeChunk(byte[] chunk) {
        if (chunk == null || chunk.length == 0) {
            return null;
        }
        boolean fin = chunk[chunk.length - 1] == ChunkCodec.SEP_ENTRY;
        int bodyEnd = fin ? chunk.length - 1 : chunk.length;
        int sep = indexOf(chunk, ChunkCodec.SEP_KV, 0);
        if (sep < 0 || sep >= bodyEnd) {
            return null;
        }
        try {
            JsonNode h = MAPPER.readTree(new String(chunk, 0, sep, StandardCharsets.UTF_8));
            String bucket = h.get("bucket").asString();
            String key = h.get("key").asString();
            long size = h.get("size").asLong();
            long version = h.get("version").asLong();
            String etag = h.get("etag").asString();
            String contentType = h.get("contentType").asString();
            long createdAt = h.get("createdAt").asLong();
            long offset = h.get("offset").asLong();
            byte[] data = Base64.getDecoder().decode(
                    new String(chunk, sep + 1, bodyEnd - sep - 1, StandardCharsets.UTF_8));
            ObjectMetadata meta = new ObjectMetadata(key, size, contentType,
                    etag == null || etag.isEmpty() ? null : etag, createdAt, version);
            return new StorageChunkView(bucket, meta, data, offset, fin);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static int indexOf(byte[] haystack, byte needle, int from) {
        for (int i = from; i < haystack.length; i++) {
            if (haystack[i] == needle) {
                return i;
            }
        }
        return -1;
    }
}
