package com.ww.runtime.data.storage;

import com.ww.runtime.data.storage.engine.ObjectMetadata;

import java.util.Objects;

/**
 * STORAGE 迁移分片视图（decode 产物）。
 *
 * @param bucket 桶名
 * @param meta   对象元数据（每片携带）
 * @param data   本片数据
 * @param offset 本片在对象内的字节偏移
 * @param fin    是否对象最后一片
 */
public record StorageChunkView(String bucket, ObjectMetadata meta, byte[] data,
                               long offset, boolean fin) {

    public StorageChunkView {
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(data, "data");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }
    }

    public String key() {
        return meta.getKey();
    }
}
