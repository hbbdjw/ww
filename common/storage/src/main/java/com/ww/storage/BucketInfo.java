package com.ww.storage;

/**
 * Bucket 元信息。以 .bucket 文件与对象数据共盘存储，无需元数据库。
 */
public class BucketInfo {

    private String name;
    private long createdAt;
    private long version;

    public BucketInfo() {}

    public BucketInfo(String name, long createdAt) {
        this.name = name;
        this.createdAt = createdAt;
        this.version = 1;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }

    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
