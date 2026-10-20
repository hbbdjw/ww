package com.ww.runtime.data.storage.engine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 对象元数据。与对象数据一起落盘（.meta 文件），无需独立元数据库。
 *
 * <p>字段说明：
 * <ul>
 *   <li>key：原始对象键（可能含 '/'）</li>
 *   <li>size：对象字节数</li>
 *   <li>contentType：MIME 类型</li>
 *   <li>etag：内容指纹（MD5 十六进制），用于校验与 S3 ETag 响应</li>
 *   <li>createdAt：创建时间戳（ms）</li>
 *   <li>version：单 (bucket,key) 维度单调递增版本号，用于 read-after-write 校验与覆盖检测</li>
 *   <li>userMetadata：S3 自定义元数据（x-amz-meta-*），原样存取</li>
 * </ul>
 */
public class ObjectMetadata {

    private String key;
    private long size;
    private String contentType = "application/octet-stream";
    private String etag;
    private long createdAt;
    private long version;
    private Map<String, String> userMetadata = new LinkedHashMap<>();

    public ObjectMetadata() {}

    public ObjectMetadata(String key, long size, String contentType, String etag, long createdAt, long version) {
        this.key = key;
        this.size = size;
        this.contentType = contentType;
        this.etag = etag;
        this.createdAt = createdAt;
        this.version = version;
    }

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }

    public long getSize() { return size; }
    public void setSize(long size) { this.size = size; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) {
        this.contentType = contentType == null ? "application/octet-stream" : contentType;
    }

    public String getEtag() { return etag; }
    public void setEtag(String etag) { this.etag = etag; }

    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }

    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }

    public Map<String, String> getUserMetadata() {
        return userMetadata == null ? Collections.emptyMap() : userMetadata;
    }

    public void setUserMetadata(Map<String, String> userMetadata) {
        this.userMetadata = userMetadata == null ? new LinkedHashMap<>() : new LinkedHashMap<>(userMetadata);
    }

    public void addUserMeta(String k, String v) {
        if (userMetadata == null) userMetadata = new LinkedHashMap<>();
        userMetadata.put(k, v);
    }
}
