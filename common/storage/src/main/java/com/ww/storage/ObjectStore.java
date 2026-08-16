package com.ww.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import tools.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 对象本地存储：将数据与元数据作为对象一起写入 Drive（.data + .meta 同目录），无需元数据库。
 *
 * <p>写入流程（read-after-write 关键）：
 * <ol>
 *   <li>定位 Drive（SetLocator 稳定散列）</li>
 *   <li>读取既有 .meta 递增 version（主写路径）</li>
 *   <li>写入 object.data.tmp，期间流式计算 MD5(plaintext) 作为 ETag 与 size</li>
 *   <li>写入 object.meta.tmp</li>
 *   <li>先 rename .data 再 rename .meta —— 元数据可见即代表数据已完整落盘</li>
 * </ol>
 * 读取：读 .meta 后读 .data，主节点本地读保证 read-after-write。
 *
 * <p>复制路径 {@link #putReplica} 使用主节点计算的 ETag/version，保证主备一致。
 *
 * <p><b>并发控制</b>：同一 (bucket,key) 的写入/删除/复制操作使用条带锁（{@link #lockFor}）
 * 互斥，保证 version 单调递增与删除一致性（防止并发写产生相同 version 导致副本分叉，
 * 防止删除与写入交错导致数据残留）。
 *
 * <p><b>性能优化</b>：默认（无加密）路径使用 {@link FileChannel} + 直接内存 {@link ByteBuffer}
 * （来自 {@link ByteBufferPool}），避免堆内 byte[] 拷贝；当源 InputStream 来自本地文件
 * （如副本复制）时，source 为 FileChannel，{@code transferFrom} 可走内核 sendfile。
 * MD5 计算通过 {@link MessageDigest#update(ByteBuffer)} 直接消费直接内存，零堆内分配。
 */
public class ObjectStore {

    private static final Logger log = LoggerFactory.getLogger(ObjectStore.class);

    private final DriveManager driveManager;
    private final SetLocator setLocator;
    private final CryptoCodec cryptoCodec;
    private final ObjectMapper objectMapper;
    private final ByteBufferPool bufferPool;
    /** MD5 MessageDigest 对象池：虚拟线程下避免 ThreadLocal 创建过多实例 */
    private final Deque<MessageDigest> md5Pool = new ConcurrentLinkedDeque<>();
    /** 对象池上限，防止无限增长 */
    private static final int MD5_POOL_MAX = 64;

    /** 条带锁：同一 (bucket,key) 的写/删/复制互斥，保证 version 单调递增与删除一致性 */
    private static final int LOCK_STRIPES = 64;
    private final Lock[] locks;
    {
        locks = new Lock[LOCK_STRIPES];
        for (int i = 0; i < LOCK_STRIPES; i++) locks[i] = new ReentrantLock();
    }
    private Lock lockFor(String bucket, String key) {
        return locks[((bucket.hashCode() * 31 + key.hashCode()) & 0x7FFFFFFF) % LOCK_STRIPES];
    }

    public ObjectStore(DriveManager driveManager, SetLocator setLocator,
                       CryptoCodec cryptoCodec, ObjectMapper objectMapper,
                       ByteBufferPool bufferPool) {
        this.driveManager = driveManager;
        this.setLocator = setLocator;
        this.cryptoCodec = cryptoCodec;
        this.objectMapper = objectMapper;
        this.bufferPool = bufferPool;
    }

    private MessageDigest borrowMd5() {
        MessageDigest d = md5Pool.poll();
        if (d != null) {
            d.reset();
            return d;
        }
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new StorageException("MD5 不可用", e);
        }
    }

    private void returnMd5(MessageDigest d) {
        if (d == null) return;
        d.reset();
        if (md5Pool.size() < MD5_POOL_MAX) {
            md5Pool.offer(d);
        }
    }

    // ==================== 主写路径 ====================

    /**
     * 主节点写入对象：计算 ETag/version 并落盘。
     * @param length 提示长度，&lt;0 表示未知（chunked）
     * @return 落盘后的句柄（含元数据与数据文件，用于复制）
     */
    public StoredObject putPrimary(String bucket, String key, InputStream data, long length,
                                  String contentType, Map<String, String> userMeta) {
        Drive.validateName(bucket, "bucket");
        if (key == null || key.isEmpty()) {
            throw new StorageException("object key 为空");
        }
        SetLocator.LocatedDrive loc = setLocator.locate(bucket, key);
        Drive drive = loc.getDrive();
        if (!drive.isOnline()) {
            throw new StorageException("目标 Drive 离线: " + drive);
        }
        // 同 key 加锁：防止并发写读到相同旧 version 产生相同新 version 导致副本分叉
        Lock lock = lockFor(bucket, key);
        lock.lock();
        try {
            long newVersion = 1;
            ObjectMetadata existing = readMeta(drive, bucket, loc.getKeySha1());
            if (existing != null) {
                newVersion = existing.getVersion() + 1;
            }
            ObjectMetadata meta = new ObjectMetadata(key, -1L, contentType, null,
                    System.currentTimeMillis(), newVersion);
            if (userMeta != null) meta.setUserMetadata(userMeta);
            return writeObject(drive, bucket, loc.getKeySha1(), data, meta);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 副本写入：使用主节点计算的元数据，保证主备一致。
     * <p>幂等保护：若副本已持有不低于本次的版本则直接跳过（支撑复制重试/对账，
     * 避免旧版本覆盖新版本或重复写盘）。
     */
    public StoredObject putReplica(String bucket, String key, InputStream data, long length,
                                   ObjectMetadata meta) {
        Drive.validateName(bucket, "bucket");
        if (key == null || key.isEmpty()) {
            throw new StorageException("object key 为空");
        }
        SetLocator.LocatedDrive loc = setLocator.locate(bucket, key);
        Drive drive = loc.getDrive();
        if (!drive.isOnline()) {
            throw new StorageException("副本 Drive 离线: " + drive);
        }
        // 同 key 加锁：与主写/删除保持一致性
        Lock lock = lockFor(bucket, key);
        lock.lock();
        try {
            ObjectMetadata existing = readMeta(drive, bucket, loc.getKeySha1());
            if (existing != null && existing.getVersion() >= meta.getVersion()) {
                return new StoredObject(existing, drive.objectDataFile(bucket, loc.getKeySha1()), bucket);
            }
            ObjectMetadata copy = new ObjectMetadata(key, meta.getSize(), meta.getContentType(),
                    meta.getEtag(), meta.getCreatedAt(), meta.getVersion());
            copy.setUserMetadata(meta.getUserMetadata());
            return writeObject(drive, bucket, loc.getKeySha1(), data, copy);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 实际落盘逻辑。
     * <p>当 meta.etag 为 null 时流式计算 MD5；当 meta.size &lt; 0 时统计字节。
     * <p>无加密路径走 {@link FileChannel} + 直接内存 {@link ByteBuffer}；加密路径保留 OutputStream。
     */
    private StoredObject writeObject(Drive drive, String bucket, String keySha1,
                                     InputStream data, ObjectMetadata meta) {
        File objDir = drive.objectDir(bucket, keySha1);
        File dataTmp;
        try {
            if (!objDir.exists() && !objDir.mkdirs()) {
                throw new StorageException("无法创建对象目录: " + objDir);
            }
            dataTmp = Drive.tempFile(objDir, "object.data");
        } catch (IOException e) {
            throw new StorageException("创建临时文件失败: " + objDir, e);
        }

        boolean computeMd5 = meta.getEtag() == null;
        MessageDigest digest = computeMd5 ? borrowMd5() : null;
        try {
            boolean needCount = meta.getSize() < 0;
            long size = meta.getSize();

            if (!cryptoCodec.enabled()) {
                // 快路径：FileChannel + 直接内存 ByteBuffer，避免堆内 byte[] 拷贝
                try {
                    size = writeViaFileChannel(dataTmp, data, digest, needCount, size);
                } catch (IOException e) {
                    safeDelete(dataTmp);
                    throw new StorageException("写入对象数据失败: bucket=" + bucket + " key=" + meta.getKey(), e);
                }
            } else {
                // 加密路径：保留 OutputStream 包装（为后续真实加密扩展预留）
                try {
                    size = writeViaStream(dataTmp, data, bucket, meta.getKey(), digest, needCount, size);
                } catch (IOException e) {
                    safeDelete(dataTmp);
                    throw new StorageException("写入对象数据失败: bucket=" + bucket + " key=" + meta.getKey(), e);
                }
            }

            if (needCount) meta.setSize(size);
            if (computeMd5) meta.setEtag(toHex(digest.digest()));

            // 写元信息 tmp
            File metaFile = drive.objectMetaFile(bucket, keySha1);
            File metaTmp;
            try {
                metaTmp = Drive.tempFile(objDir, "object.meta");
                objectMapper.writeValue(metaTmp, meta);
            } catch (Exception e) {
                safeDelete(dataTmp);
                throw new StorageException("写入对象元信息失败: " + meta.getKey(), e);
            }

            // 先暴露数据再暴露元信息：元信息可见即代表数据已完整落盘
            File dataFile = drive.objectDataFile(bucket, keySha1);
            if (!atomicRename(dataTmp, dataFile) || !atomicRename(metaTmp, metaFile)) {
                safeDelete(dataTmp);
                safeDelete(metaTmp);
                throw new StorageException("对象改名失败: " + meta.getKey());
            }
            return new StoredObject(meta, dataFile, bucket);
        } finally {
            returnMd5(digest);
        }
    }

    /**
     * 快路径写入：使用 {@link FileChannel} + 池化直接内存 {@link ByteBuffer}。
     *
     * <p>分两种情形：
     * <ul>
     *   <li><b>源为 {@link FileInputStream} 且无需 MD5</b>（即副本复制路径，etag 已由主节点计算）：
     *       直接调用 {@link FileChannel#transferFrom(ReadableByteChannel, long, long)}，
     *       走内核 sendfile，零 JVM 拷贝、零用户态上下文切换</li>
     *   <li><b>其他情形</b>（主写需算 MD5，或源为通用 InputStream）：
     *       通过池化直接内存 {@link ByteBuffer} 中转，{@link FileChannel#write(ByteBuffer)}
     *       写入文件，MD5 经 {@link MessageDigest#update(ByteBuffer)} 直接消费直接内存</li>
     * </ul>
     */
    private long writeViaFileChannel(File dataTmp, InputStream data,
                                     MessageDigest digest, boolean needCount, long initialSize) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(dataTmp);
             FileChannel fc = fos.getChannel()) {
            long size = Math.max(0, initialSize);

            // 情形 1：源为 FileChannel 且无需计算 MD5 → 纯内核 sendfile
            if (data instanceof FileInputStream fis && digest == null) {
                FileChannel srcFc = fis.getChannel();
                long remaining = srcFc.size();
                long position = 0;
                while (remaining > 0) {
                    long t = fc.transferFrom(srcFc, position, remaining);
                    if (t <= 0) break;
                    position += t;
                    remaining -= t;
                }
                if (needCount) size = position;
                fc.force(false);
                return size;
            }

            // 情形 2：通用路径，经直接内存 ByteBuffer 中转（MD5 直接消费直接内存）
            ReadableByteChannel src = (data instanceof FileInputStream fis)
                    ? fis.getChannel()
                    : Channels.newChannel(data);
            try (ByteBufferPool.LeasedBuffer lb = bufferPool.borrow()) {
                ByteBuffer buf = lb.buffer();
                while (true) {
                    buf.clear();
                    int n = src.read(buf);
                    if (n == -1) break;
                    if (n == 0) continue;
                    buf.flip();
                    if (digest != null) {
                        ByteBuffer dup = buf.duplicate();
                        digest.update(dup);
                    }
                    while (buf.hasRemaining()) fc.write(buf);
                    if (needCount) size += n;
                }
            }
            fc.force(false);
            return size;
        }
    }

    /**
     * 加密路径写入：保留 OutputStream 包装以接入未来真实加密实现。
     * <p>使用堆内 byte[]（加密算法通常需 byte 访问），缓冲区大小与池对齐。
     */
    private long writeViaStream(File dataTmp, InputStream data, String bucket, String key,
                                MessageDigest digest, boolean needCount, long initialSize) throws IOException {
        long size = Math.max(0, initialSize);
        try (OutputStream raw = Files.newOutputStream(dataTmp.toPath());
             OutputStream out = cryptoCodec.enabled()
                     ? cryptoCodec.encryptWrite(raw, bucket, key)
                     : raw) {
            byte[] buf = new byte[bufferPool.getBufferSize()];
            int n;
            while ((n = data.read(buf)) != -1) {
                out.write(buf, 0, n);
                if (digest != null) digest.update(buf, 0, n);
                if (needCount) size += n;
            }
            out.flush();
        }
        return size;
    }

    // ==================== 读路径 ====================

    /** 读取对象：返回元数据 + 数据文件的句柄（流由调用方按 openRead 打开） */
    public StoredObject get(String bucket, String key) {
        SetLocator.LocatedDrive loc = setLocator.locate(bucket, key);
        Drive drive = loc.getDrive();
        ObjectMetadata meta = readMeta(drive, bucket, loc.getKeySha1());
        if (meta == null) return null;
        File dataFile = drive.objectDataFile(bucket, loc.getKeySha1());
        if (!dataFile.exists()) {
            log.warn("对象元信息存在但数据缺失: bucket={} key={}", bucket, key);
            return null;
        }
        return new StoredObject(meta, dataFile, bucket);
    }

    /** 仅读元数据（HEAD） */
    public ObjectMetadata head(String bucket, String key) {
        SetLocator.LocatedDrive loc = setLocator.locate(bucket, key);
        return readMeta(loc.getDrive(), bucket, loc.getKeySha1());
    }

    /**
     * 打开解密读流。
     * <p>使用池化直接内存缓冲区大小的 {@link BufferedInputStream}（默认 64KB），
     * 替代 JDK 默认的 8KB，减少系统调用次数。
     */
    public InputStream openRead(StoredObject obj) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(obj.dataFile), bufferPool.getBufferSize());
        if (cryptoCodec.enabled()) {
            return cryptoCodec.decryptRead(in, obj.bucket, obj.meta.getKey());
        }
        return in;
    }

    /** 删除对象：同 key 加锁，先删 .meta（使对象对读不可见）再删 .data，
     *  与写入顺序（先 data 后 meta）对称，消除并发窗口内假 404 */
    public boolean delete(String bucket, String key) {
        Drive.validateName(bucket, "bucket");
        if (key == null || key.isEmpty()) {
            throw new StorageException("object key 为空");
        }
        SetLocator.LocatedDrive loc = setLocator.locate(bucket, key);
        Drive drive = loc.getDrive();
        Lock lock = lockFor(bucket, key);
        lock.lock();
        try {
            File metaFile = drive.objectMetaFile(bucket, loc.getKeySha1());
            File dataFile = drive.objectDataFile(bucket, loc.getKeySha1());
            boolean metaDeleted = metaFile.exists() && metaFile.delete();
            boolean dataDeleted = dataFile.exists() && dataFile.delete();
            cleanupEmptyDirs(drive.objectDir(bucket, loc.getKeySha1()), drive.objectsDir(bucket));
            return metaDeleted || dataDeleted;
        } finally {
            lock.unlock();
        }
    }

    /** 列举对象：扫描桶在所有在线 Drive 上的对象目录，按 key 排序后按 prefix/marker/maxKeys 过滤 */
    public ListResult list(String bucket, String prefix, String marker, int maxKeys) {
        if (maxKeys <= 0) maxKeys = 1000;
        TreeMap<String, ObjectMetadata> dedup = new TreeMap<>();
        for (Drive d : driveManager.getDrives()) {
            if (!d.isOnline()) continue;
            File base = d.objectsDir(bucket);
            if (!base.exists()) continue;
            scanObjects(base, dedup);
        }
        List<ObjectMetadata> all = new ArrayList<>(dedup.values());
        List<ObjectMetadata> out = new ArrayList<>();
        String nextMarker = null;
        boolean truncated = false;
        for (ObjectMetadata m : all) {
            if (prefix != null && !m.getKey().startsWith(prefix)) continue;
            if (marker != null && m.getKey().compareTo(marker) <= 0) continue;
            if (out.size() >= maxKeys) {
                truncated = true;
                nextMarker = out.getLast().getKey();
                break;
            }
            out.add(m);
        }
        return new ListResult(out, nextMarker, truncated);
    }

    // ==================== 内部工具 ====================

    private void scanObjects(File dir, TreeMap<String, ObjectMetadata> sink) {
        File[] cs = dir.listFiles();
        if (cs == null) return;
        for (File c : cs) {
            if (c.isDirectory()) {
                scanObjects(c, sink);
            } else if ("object.meta".equals(c.getName())) {
                try {
                    ObjectMetadata m = objectMapper.readValue(c, ObjectMetadata.class);
                    if (m != null && m.getKey() != null) {
                        sink.putIfAbsent(m.getKey(), m);
                    }
                } catch (Exception e) {
                    log.warn("读取对象元信息失败: {}", c, e);
                }
            }
        }
    }

    private ObjectMetadata readMeta(Drive drive, String bucket, String keySha1) {
        File f = drive.objectMetaFile(bucket, keySha1);
        if (!f.exists()) return null;
        try {
            return objectMapper.readValue(f, ObjectMetadata.class);
        } catch (Exception e) {
            log.warn("读取对象元信息失败: {}", f, e);
            return null;
        }
    }

    /** 原子改名：使用 {@link java.nio.file.Files#move} + REPLACE_EXISTING，
     *  同目录下在 POSIX 文件系统上保证原子替换（包括 Windows NTFS） */
    private static boolean atomicRename(File src, File dst) {
        try {
            Files.move(src.toPath(), dst.toPath(),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException e) {
            log.warn("原子改名失败: {} -> {}: {}", src, dst, e.getMessage());
            return false;
        }
    }

    private static void safeDelete(File f) {
        if (f != null && f.exists()) f.delete();
    }

    private static void cleanupEmptyDirs(File leaf, File stopAt) {
        File cur = leaf;
        while (cur != null && !cur.equals(stopAt) && cur.exists()) {
            File[] cs = cur.listFiles();
            if (cs != null && cs.length == 0) {
                if (!cur.delete()) break;
            } else {
                break;
            }
            cur = cur.getParentFile();
        }
    }

    private static String toHex(byte[] bytes) {
        char[] hex = new char[bytes.length * 2];
        String digits = "0123456789abcdef";
        for (int i = 0; i < bytes.length; i++) {
            hex[i * 2] = digits.charAt((bytes[i] >> 4) & 0xF);
            hex[i * 2 + 1] = digits.charAt(bytes[i] & 0xF);
        }
        return new String(hex);
    }

    /** 读取结果句柄 */
    public static class StoredObject {
        public final ObjectMetadata meta;
        public final File dataFile;
        public final String bucket;

        StoredObject(ObjectMetadata meta, File dataFile, String bucket) {
            this.meta = meta;
            this.dataFile = dataFile;
            this.bucket = bucket;
        }
    }

    /** 列举结果 */
    public static class ListResult {
        private final List<ObjectMetadata> objects;
        private final String nextMarker;
        private final boolean truncated;

        ListResult(List<ObjectMetadata> objects, String nextMarker, boolean truncated) {
            this.objects = objects;
            this.nextMarker = nextMarker;
            this.truncated = truncated;
        }

        public List<ObjectMetadata> getObjects() { return objects; }
        public String getNextMarker() { return nextMarker; }
        public boolean isTruncated() { return truncated; }
    }
}
