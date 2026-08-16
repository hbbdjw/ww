package com.ww.storage;

import java.io.File;
import java.io.IOException;

/**
 * Drive（磁盘）：本地存储数据的物理磁盘/目录。启动时由配置文件传入路径。
 *
 * <p>所有对象数据最终落盘于某个 Drive。每个 Drive 持有自身的 buckets/objects 目录树。
 * online 标记磁盘可用性，离线 Drive 不参与新对象写入（已有数据仍可读，直至被迁移）。
 */
public class Drive {

    private final int index;
    private final File root;
    private volatile boolean online = true;

    public Drive(int index, String rootPath) {
        this.index = index;
        this.root = new File(rootPath).getAbsoluteFile();
    }

    public int getIndex() { return index; }

    public File getRoot() { return root; }

    public String getRootPath() { return root.getAbsolutePath(); }

    public boolean isOnline() { return online; }

    public void setOnline(boolean online) { this.online = online; }

    /** 确保磁盘根目录存在并可写 */
    public void ensureExists() {
        if (!root.exists() && !root.mkdirs()) {
            throw new StorageException("无法创建 Drive 目录: " + root);
        }
        if (!root.isDirectory()) {
            throw new StorageException("Drive 路径不是目录: " + root);
        }
        if (!root.canWrite()) {
            throw new StorageException("Drive 目录不可写: " + root);
        }
    }

    /** 该 Drive 下存放 bucket 元信息与对象的根目录 */
    public File bucketsDir() {
        return new File(root, "buckets");
    }

    public File bucketDir(String bucket) {
        validateName(bucket, "bucket");
        return new File(bucketsDir(), bucket);
    }

    /** 桶元信息文件 */
    public File bucketMetaFile(String bucket) {
        return new File(bucketDir(bucket), ".bucket");
    }

    /** 桶对象数据根目录 */
    public File objectsDir(String bucket) {
        return new File(bucketDir(bucket), "objects");
    }

    /**
     * 对象存储目录：按 key 的 SHA-1 做 2 级散列，避免单目录文件爆炸与路径穿越。
     * <pre>
     *   objects/ab/cd/&lt;full-sha1&gt;/object.data
     *   objects/ab/cd/&lt;full-sha1&gt;/object.meta
     * </pre>
     */
    public File objectDir(String bucket, String keySha1) {
        if (keySha1 == null || keySha1.length() < 4) {
            throw new StorageException("keySha1 长度不足: " + keySha1);
        }
        File base = objectsDir(bucket);
        return new File(new File(base, keySha1.substring(0, 2)), keySha1.substring(2, 4))
                .toPath().resolve(keySha1).toFile();
    }

    public File objectDataFile(String bucket, String keySha1) {
        return new File(objectDir(bucket, keySha1), "object.data");
    }

    public File objectMetaFile(String bucket, String keySha1) {
        return new File(objectDir(bucket, keySha1), "object.meta");
    }

    /** 原子写入临时文件后改名，避免读到半成品 */
    public static File tempFile(File dir, String prefix) throws IOException {
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("无法创建目录: " + dir);
        }
        return File.createTempFile(prefix, ".tmp", dir);
    }

    /** 名称合法性校验：禁止路径穿越与非法字符 */
    public static void validateName(String name, String what) {
        if (name == null || name.isEmpty()) {
            throw new StorageException(what + " 名称为空");
        }
        if (name.contains("/") || name.contains("\\") || name.contains("\0")) {
            throw new StorageException(what + " 名称含非法字符: " + name);
        }
        if (name.equals(".") || name.equals("..")) {
            throw new StorageException(what + " 名称非法: " + name);
        }
    }

    @Override
    public String toString() {
        return "Drive[" + index + "]" + root + (online ? "" : "(offline)");
    }
}
