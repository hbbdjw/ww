package com.ww.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tools.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bucket 元信息本地存储：将 BucketInfo 以 .bucket 文件与对象数据共盘存储，无需元数据库。
 *
 * <p>Bucket 元信息在所有在线 Drive 上冗余写入（任一 Drive 损坏仍可恢复元信息），
 * 读取时以任一可用 Drive 的 .bucket 为准。Bucket 目录同时作为对象的命名空间。
 */
public class BucketStore {

    private static final Logger log = LoggerFactory.getLogger(BucketStore.class);

    private final DriveManager driveManager;
    private final ObjectMapper objectMapper;

    public BucketStore(DriveManager driveManager, ObjectMapper objectMapper) {
        this.driveManager = driveManager;
        this.objectMapper = objectMapper;
    }

    /** 创建 Bucket：在所有在线 Drive 上建立目录并写入 .bucket 元信息 */
    public BucketInfo createBucket(String name) {
        Drive.validateName(name, "bucket");
        if (exists(name)) {
            throw new StorageException("Bucket 已存在: " + name);
        }
        BucketInfo info = new BucketInfo(name, System.currentTimeMillis());
        List<Drive> drives = driveManager.getDrives();
        for (Drive d : drives) {
            if (!d.isOnline()) continue;
            File dir = d.bucketDir(name);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new StorageException("无法创建 Bucket 目录: " + dir);
            }
            writeMeta(d, name, info);
        }
        log.info("Bucket 已创建: {} (drives={})", name, drives.size());
        return info;
    }

    /** 删除 Bucket：要求为空（无对象）。recursive=true 时强制清空 */
    public void deleteBucket(String name, boolean recursive) {
        Drive.validateName(name, "bucket");
        if (!exists(name)) {
            throw new StorageException("Bucket 不存在: " + name);
        }
        for (Drive d : driveManager.getDrives()) {
            if (!d.isOnline()) continue;
            File bdir = d.bucketDir(name);
            if (!bdir.exists()) continue;
            if (!recursive) {
                File odir = d.objectsDir(name);
                if (odir.exists() && hasObject(odir)) {
                    throw new StorageException("Bucket 非空，无法删除: " + name);
                }
            }
            deleteTree(bdir);
        }
        log.info("Bucket 已删除: {} (recursive={})", name, recursive);
    }

    public boolean exists(String name) {
        Drive.validateName(name, "bucket");
        for (Drive d : driveManager.getDrives()) {
            if (d.isOnline() && d.bucketMetaFile(name).exists()) {
                return true;
            }
        }
        return false;
    }

    public BucketInfo getBucketInfo(String name) {
        Drive.validateName(name, "bucket");
        for (Drive d : driveManager.getDrives()) {
            if (!d.isOnline()) continue;
            File f = d.bucketMetaFile(name);
            BucketInfo info = readMeta(f);
            if (info != null) return info;
        }
        return null;
    }

    /** 列出所有 Bucket（跨 Drive 去重） */
    public List<BucketInfo> listBuckets() {
        Map<String, BucketInfo> byName = new LinkedHashMap<>();
        for (Drive d : driveManager.getDrives()) {
            if (!d.isOnline()) continue;
            File base = d.bucketsDir();
            File[] children = base.listFiles();
            if (children == null) continue;
            for (File c : children) {
                if (!c.isDirectory()) continue;
                BucketInfo info = readMeta(d.bucketMetaFile(c.getName()));
                if (info != null) {
                    byName.putIfAbsent(info.getName(), info);
                }
            }
        }
        return new ArrayList<>(byName.values());
    }

    // ---------------- 内部 ----------------

    /** 写入 Bucket 元信息：使用 {@link Files#move} + REPLACE_EXISTING 原子替换，
     *  同目录同文件系统上保证原子性 */
    private void writeMeta(Drive d, String name, BucketInfo info) {
        File meta = d.bucketMetaFile(name);
        File tmp;
        try {
            tmp = Drive.tempFile(d.bucketDir(name), ".bucket");
            objectMapper.writeValue(tmp, info);
        } catch (Exception e) {
            throw new StorageException("写入 Bucket 元信息失败: " + name, e);
        }
        try {
            Files.move(tmp.toPath(), meta.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new StorageException("Bucket 元信息改名失败: " + meta, e);
        }
    }

    private BucketInfo readMeta(File meta) {
        if (!meta.exists()) return null;
        try {
            return objectMapper.readValue(meta, BucketInfo.class);
        } catch (Exception e) {
            log.warn("读取 Bucket 元信息失败: {}", meta, e);
            return null;
        }
    }

    private static boolean hasObject(File objectsDir) {
        File[] l1 = objectsDir.listFiles();
        if (l1 == null) return false;
        for (File a : l1) {
            if (!a.isDirectory()) continue;
            File[] l2 = a.listFiles();
            if (l2 == null) continue;
            for (File b : l2) {
                if (!b.isDirectory()) continue;
                File[] l3 = b.listFiles();
                if (l3 == null) continue;
                for (File c : l3) {
                    if (c.isDirectory() && new File(c, "object.meta").exists()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static void deleteTree(File f) {
        if (f == null || !f.exists()) return;
        if (f.isDirectory()) {
            File[] cs = f.listFiles();
            if (cs != null) {
                for (File c : cs) deleteTree(c);
            }
        }
        if (!f.delete()) {
            log.warn("无法删除: {}", f);
        }
    }
}
