package com.ww.runtime.data.storage.engine;

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
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.ObjectMapper;

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
 * <p>列举/计数不经磁盘：启动时 {@link #rebuildIndex()} 从全部 {@code object.meta}
 * 一次性重建内存有序索引（{@link MetadataIndex}），此后写/删增量维护；
 * 索引容量满后新增被拒，受影响桶列举降级目录扫描（T3 止血：正确性优先）。
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

    /**
     * {@link java.io.FilterInputStream#in} 的反射访问器：JDK 21 无公开 {@code in()} 访问器，
     * 解开 {@link java.io.BufferedInputStream} 等包装以取回底层 {@link FileInputStream}（启用 sendfile）
     * 只能反射读取该 protected 字段。模块系统拒绝访问（未开 --add-opens）时为 null，
     * 此时 {@link #unwrapToUnderlying} 原样返回，sendfile 快路径安全退化为通用路径。
     */
    private static final java.lang.reflect.Field FILTER_STREAM_IN = filterStreamInField();

    private static java.lang.reflect.Field filterStreamInField() {
        try {
            java.lang.reflect.Field f = java.io.FilterInputStream.class.getDeclaredField("in");
            f.setAccessible(true);
            return f;
        } catch (Throwable t) {
            return null;
        }
    }

    /** 逐层解开 {@link java.io.FilterInputStream} 包装，尝试取回底层流（无法解开时原样返回）。 */
    private static InputStream unwrapToUnderlying(InputStream in) {
        if (FILTER_STREAM_IN == null) return in;
        InputStream cur = in;
        while (cur instanceof java.io.FilterInputStream) {
            try {
                InputStream next = (InputStream) FILTER_STREAM_IN.get(cur);
                if (next == null || next == cur) break;
                cur = next;
            } catch (Throwable t) {
                break;
            }
        }
        return cur;
    }

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

    /**
     * 对象元数据索引（T3：抽出 {@link MetadataIndex} 承载容量上限与降级语义）。
     *
     * <p>元数据落盘为散列目录下的 {@code object.meta} 文件（目录名 = SHA-1(key)，与 key
     * 顺序无关），无法按 prefix/marker 剪枝——列举若走全盘扫描则成本 O(全部对象数)，
     * 分页形同虚设。索引在 {@link #rebuildIndex()}（能力启动时）从磁盘一次性重建，
     * 此后写/删在条带锁内、文件 rename 成功后增量维护，与磁盘严格一致；
     * 列举/计数经索引定位降为 O(log N + pageSize)。
     *
     * <p>按 Drive 隔离是为了保持列举的既有语义：离线 Drive 上的对象不参与列举/计数。
     * 重建对所有 Drive 执行（含离线的），Drive 恢复在线后无需再建。
     */
    private final MetadataIndex metadataIndex;

    /** T13：副本路径 fsync 开关（默认 false = 与主路径一致做 fsync；磁盘密集场景可配置跳过） */
    private volatile boolean replicaSkipFsync = false;
    /** T26：同 version 异 etag 冲突计数（默认策略：保留先到 + 告警留痕） */
    private final java.util.concurrent.atomic.AtomicLong sameVersionConflicts =
            new java.util.concurrent.atomic.AtomicLong();
    /** T15：损坏元数据隔离计数 */
    private final java.util.concurrent.atomic.AtomicLong corruptedMetaQuarantined =
            new java.util.concurrent.atomic.AtomicLong();

    public ObjectStore(DriveManager driveManager, SetLocator setLocator,
                       CryptoCodec cryptoCodec, ObjectMapper objectMapper,
                       ByteBufferPool bufferPool) {
        this(driveManager, setLocator, cryptoCodec, objectMapper, bufferPool,
                new MetadataIndex(0)); // 兼容构造：不限容量（测试/单机小场景）
    }

    public ObjectStore(DriveManager driveManager, SetLocator setLocator,
                       CryptoCodec cryptoCodec, ObjectMapper objectMapper,
                       ByteBufferPool bufferPool, MetadataIndex metadataIndex) {
        this.driveManager = driveManager;
        this.setLocator = setLocator;
        this.cryptoCodec = cryptoCodec;
        this.objectMapper = objectMapper;
        this.bufferPool = bufferPool;
        this.metadataIndex = metadataIndex;
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

    /** T13：副本写路径 fsync 配置（true=跳过 fsync，默认 false 与主路径一致）。 */
    public void setReplicaSkipFsync(boolean replicaSkipFsync) {
        this.replicaSkipFsync = replicaSkipFsync;
    }

    /** T26：同 version 异 etag 冲突计数（观测口）。 */
    public long getSameVersionConflicts() {
        return sameVersionConflicts.get();
    }

    /** T15：损坏元数据隔离计数（观测口）。 */
    public long getCorruptedMetaQuarantined() {
        return corruptedMetaQuarantined.get();
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
            } else if (drive.objectDataFile(bucket, loc.getKeySha1()).exists()) {
                // T15：有 data 无合法 meta（元数据损坏被隔离）——拒绝覆盖写，
                // 防止 version 静默重置为 1 造成主备分叉；需人工恢复后才能重建
                throw new StorageException("对象数据存在但元数据缺失/损坏，拒绝覆盖写（防版本重置）: "
                        + bucket + "/" + key);
            }
            ObjectMetadata meta = new ObjectMetadata(key, -1L, contentType, null,
                    System.currentTimeMillis(), newVersion);
            if (userMeta != null) meta.setUserMetadata(userMeta);
            StoredObject stored = writeObject(drive, bucket, loc.getKeySha1(), data, meta, false);
            // rename 成功后登记索引（锁内）：列举即时可见，与磁盘严格一致；
            // T3：容量满被拒 → 该桶列举降级目录扫描（读路径不受影响）
            if (!metadataIndex.putGuarded(drive, bucket, key, stored.meta)) {
                metadataIndex.degradeBucket(bucket);
            }
            return stored;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 副本写入：使用主节点计算的元数据，保证主备一致。
     * <p>幂等保护：若副本已持有不低于本次的版本则直接跳过（支撑复制重试/对账，
     * 避免旧版本覆盖新版本或重复写盘）。
     *
     * @param maxObjectSize 对象大小上限；置于版本跳过校验之后生效——合法存量对象的
     *                      冗余重放保持幂等成功，仅新增超限写被拒（T12）。
     */
    public StoredObject putReplica(String bucket, String key, InputStream data, long length,
                                   ObjectMetadata meta, long maxObjectSize) {
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
            if (existing != null && existing.getVersion() > meta.getVersion()) {
                return new StoredObject(existing, drive.objectDataFile(bucket, loc.getKeySha1()), bucket);
            }
            if (existing != null && existing.getVersion() == meta.getVersion()) {
                boolean sameContent = meta.getEtag() == null || existing.getEtag() == null
                        || Objects.equals(existing.getEtag(), meta.getEtag());
                if (sameContent) {
                    return new StoredObject(existing, drive.objectDataFile(bucket, loc.getKeySha1()), bucket);
                }
                // T26/P1-6：同 version 异 etag = 双主/脑裂遗留冲突。主节点是权威方，
                // 以本次推送内容覆盖（否则先到者永久占据副本，对账只比 version 永远修不好）。
                sameVersionConflicts.incrementAndGet();
                log.warn("T26 同版本 ETag 冲突（主端权威覆盖）: bucket={} key={} version={} "
                                + "existingEtag={} incomingEtag={}",
                        bucket, key, meta.getVersion(), existing.getEtag(), meta.getEtag());
            }
            // T12：大小上限校验置于版本跳过之后，避免误伤合法存量对象的冗余重放
            if (length > maxObjectSize) {
                throw new StorageException("object exceeds max size " + maxObjectSize + ": " + length);
            }
            ObjectMetadata copy = new ObjectMetadata(key, meta.getSize(), meta.getContentType(),
                    meta.getEtag(), meta.getCreatedAt(), meta.getVersion());
            copy.setUserMetadata(meta.getUserMetadata());
            StoredObject stored = writeObject(drive, bucket, loc.getKeySha1(), data, copy, replicaSkipFsync);
            // rename 成功后登记索引（锁内）：与防回退语义一致，仅保留不低于既有的版本；
            // T3：容量满被拒 → 该桶列举降级目录扫描（读路径不受影响）
            if (!metadataIndex.mergeGuarded(drive, bucket, key, stored.meta)) {
                metadataIndex.degradeBucket(bucket);
            }
            return stored;
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
                                     InputStream data, ObjectMetadata meta, boolean skipFsync) {
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
                    size = writeViaFileChannel(dataTmp, data, digest, needCount, size, skipFsync);
                } catch (IOException e) {
                    safeDelete(dataTmp);
                    throw new StorageException("写入对象数据失败: bucket=" + bucket + " key=" + meta.getKey(), e);
                }
            } else {
                // 加密路径：保留 OutputStream 包装（为后续真实加密扩展预留）
                try {
                    size = writeViaStream(dataTmp, data, bucket, meta.getKey(), digest, needCount, size, skipFsync);
                } catch (IOException e) {
                    safeDelete(dataTmp);
                    throw new StorageException("写入对象数据失败: bucket=" + bucket + " key=" + meta.getKey(), e);
                }
            }

            if (needCount) meta.setSize(size);
            if (computeMd5) meta.setEtag(toHex(digest.digest()));

            // T13：数据文件 fsync 统一收口（快路径内部已 force；流路径在此补齐）
            if (!skipFsync) {
                fsyncFileQuietly(dataTmp);
            }

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
            // T13：元数据 fsync——meta 可见即代表数据+元数据均已持久化
            if (!skipFsync) {
                fsyncFileQuietly(metaTmp);
            }

            // T5 覆盖写两段提交：先备份旧 data，再依次暴露新 data/meta；
            // meta 改名失败可完整回滚（消除"新数据 + 旧 meta"错配对窗口）
            File dataFile = drive.objectDataFile(bucket, keySha1);
            File dataOld = null;
            if (dataFile.exists()) {
                try {
                    dataOld = Drive.tempFile(objDir, "object.data.old");
                } catch (IOException e) {
                    safeDelete(dataTmp);
                    safeDelete(metaTmp);
                    throw new StorageException("旧数据备份文件创建失败: " + meta.getKey(), e);
                }
                if (!atomicRename(dataFile, dataOld)) {
                    safeDelete(dataTmp);
                    safeDelete(metaTmp);
                    safeDelete(dataOld);
                    throw new StorageException("旧数据备份改名失败: " + meta.getKey());
                }
            }
            if (!atomicRename(dataTmp, dataFile)) {
                if (dataOld != null) {
                    atomicRename(dataOld, dataFile); // 回滚旧数据
                }
                safeDelete(metaTmp);
                throw new StorageException("对象数据改名失败: " + meta.getKey());
            }
            if (!atomicRename(metaTmp, metaFile)) {
                // meta 改名失败：删新数据 + 恢复旧数据 + 保留旧 meta（旧读写对完好）
                safeDelete(dataFile);
                if (dataOld != null) {
                    atomicRename(dataOld, dataFile);
                }
                safeDelete(metaTmp);
                throw new StorageException("对象元信息改名失败: " + meta.getKey());
            }
            safeDelete(dataOld);
            // T13：rename 后目录 fsync，保证掉电后目录项可见（Windows 目录 fsync 不支持，静默跳过）
            if (!skipFsync) {
                fsyncDirQuietly(objDir);
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
                                     MessageDigest digest, boolean needCount, long initialSize,
                                     boolean skipFsync) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(dataTmp);
             FileChannel fc = fos.getChannel()) {
            long size = Math.max(0, initialSize);

            // 情形 1：源为 FileChannel 且无需计算 MD5 → 纯内核 sendfile
            // 先解开 BufferedInputStream 等 FilterInputStream 包装，取回底层 FileInputStream
            InputStream unwrapped = unwrapToUnderlying(data);
            if (unwrapped instanceof FileInputStream fis && digest == null) {
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
                return size;
            }

            // 情形 2：通用路径，经直接内存 ByteBuffer 中转（MD5 直接消费直接内存）
            ReadableByteChannel src = (unwrapped instanceof FileInputStream fis)
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
            return size;
        }
    }

    /**
     * 加密路径写入：保留 OutputStream 包装以接入未来真实加密实现。
     * <p>使用堆内 byte[]（加密算法通常需 byte 访问），缓冲区大小与池对齐。
     */
    private long writeViaStream(File dataTmp, InputStream data, String bucket, String key,
                                MessageDigest digest, boolean needCount, long initialSize,
                                boolean skipFsync) throws IOException {
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
            if (!skipFsync) out.flush();
        }
        return size;
    }

    // ==================== 读路径 ====================

    /** 读取对象：返回元数据 + 数据文件的句柄（流由调用方按 openRead 打开） */
    public StoredObject get(String bucket, String key) {
        SetLocator.LocatedDrive loc = setLocator.locate(bucket, key);
        Drive drive = loc.getDrive();
        ObjectMetadata meta = readMeta(drive, bucket, loc.getKeySha1());
        if (meta == null) {
            File dataFile = drive.objectDataFile(bucket, loc.getKeySha1());
            if (dataFile.exists()) {
                // T15：有 data 无合法 meta → 显式失败（映射 500）而非 404，
                // 防止对象被"不存在"误判后静默重建
                throw new StorageException("对象数据存在但元数据缺失/损坏: bucket=" + bucket + " key=" + key);
            }
            return null;
        }
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
        Drive drive = loc.getDrive();
        ObjectMetadata meta = readMeta(drive, bucket, loc.getKeySha1());
        if (meta == null && drive.objectDataFile(bucket, loc.getKeySha1()).exists()) {
            // T15：与 get 同口径——孤儿数据 HEAD 也显式失败（映射 500）
            throw new StorageException("对象数据存在但元数据缺失/损坏: bucket=" + bucket + " key=" + key);
        }
        return meta;
    }

    /**
     * T18：带缓存元数据的读取——命中调用方缓存时跳过盘上 readMeta（GET 写穿缓存路径）。
     * 数据文件不存在返回 null（缓存条目将随下次删除写穿/失效收敛）。
     */
    public StoredObject get(String bucket, String key, ObjectMetadata cachedMeta) {
        if (cachedMeta == null) {
            return get(bucket, key);
        }
        SetLocator.LocatedDrive loc = setLocator.locate(bucket, key);
        Drive drive = loc.getDrive();
        File dataFile = drive.objectDataFile(bucket, loc.getKeySha1());
        if (!dataFile.exists()) {
            return null;
        }
        return new StoredObject(cachedMeta, dataFile, bucket);
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
            // 同步摘除索引（锁内）：已删对象不再出现在列举/计数中（降级桶无索引条目，no-op 无害）
            metadataIndex.remove(drive, bucket, key);
            return metaDeleted || dataDeleted;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 列举对象：经内存索引定位（不扫描磁盘）。
     *
     * <p>对每个在线 Drive 的索引，按 prefix 取子区间、自 marker（排他）起取至多
     * maxKeys 条早停，跨 Drive 合并并按 key 排序，最后截断到 maxKeys 给出
     * nextMarker/truncated。复杂度 O(D×(log N + maxKeys))（D 为在线 Drive 数），
     * 与桶内对象总数无关。
     *
     * <p>T3：索引容量满后降级的桶改走目录扫描（语义不变：同口径过滤/排序/分页），
     * 正确性优先于 O(logN)。
     */
    public ListResult list(String bucket, String prefix, String marker, int maxKeys) {
        if (maxKeys <= 0) maxKeys = 1000;
        boolean hasPrefix = prefix != null && !prefix.isEmpty();
        String upper = hasPrefix ? prefixUpperBound(prefix) : null;
        boolean hasMarker = marker != null && !marker.isEmpty();
        TreeMap<String, ObjectMetadata> merged = new TreeMap<>();
        if (metadataIndex.isDegraded(bucket)) {
            collectScan(bucket, hasPrefix ? prefix : null, upper, hasMarker ? marker : null, merged);
        } else {
            for (Drive d : driveManager.getDrives()) {
                if (!d.isOnline()) continue;
                NavigableMap<String, ObjectMetadata> map = metadataIndex.peekIndex(d, bucket);
                if (map == null || map.isEmpty()) continue;
                NavigableMap<String, ObjectMetadata> view = map;
                if (hasPrefix) {
                    view = upper == null ? map.tailMap(prefix, true) : map.subMap(prefix, true, upper, false);
                }
                if (hasMarker) {
                    view = view.tailMap(marker, false);
                }
                int taken = 0;
                for (Map.Entry<String, ObjectMetadata> e : view.entrySet()) {
                    if (taken >= maxKeys) break;
                    // SetLocator 稳定散列到单一 Drive，同 key 不跨 Drive；merge 仅作兜底
                    merged.merge(e.getKey(), e.getValue(), (a, b) -> a.getVersion() >= b.getVersion() ? a : b);
                    taken++;
                }
            }
        }
        List<ObjectMetadata> out = new ArrayList<>(merged.values());
        boolean truncated = false;
        String nextMarker = null;
        if (out.size() > maxKeys) {
            out = new ArrayList<>(out.subList(0, maxKeys));
            truncated = true;
            nextMarker = out.getLast().getKey();
        }
        return new ListResult(out, nextMarker, truncated);
    }

    /** 桶内对象全量快照（索引遍历，按 key 排序）：复制对账/迁移清扫等共用；降级桶走目录扫描。 */
    public List<ObjectMetadata> snapshot(String bucket) {
        if (metadataIndex.isDegraded(bucket)) {
            TreeMap<String, ObjectMetadata> scanned = new TreeMap<>();
            collectScan(bucket, null, null, null, scanned);
            return new ArrayList<>(scanned.values());
        }
        TreeMap<String, ObjectMetadata> merged = new TreeMap<>();
        for (Drive d : driveManager.getDrives()) {
            if (!d.isOnline()) continue;
            NavigableMap<String, ObjectMetadata> map = metadataIndex.peekIndex(d, bucket);
            if (map != null) {
                merged.putAll(map);
            }
        }
        return new ArrayList<>(merged.values());
    }

    /** 指定桶对象数（索引计数；与 {@link #list} 同口径——不计离线 Drive）；降级桶走目录扫描。 */
    public long countObjects(String bucket) {
        if (metadataIndex.isDegraded(bucket)) {
            return snapshot(bucket).size();
        }
        long count = 0L;
        for (Drive d : driveManager.getDrives()) {
            if (!d.isOnline()) continue;
            NavigableMap<String, ObjectMetadata> map = metadataIndex.peekIndex(d, bucket);
            if (map != null) count += map.size();
        }
        return count;
    }

    /** 本节点全部桶的对象总数（索引计数；降级桶经目录扫描补差，口径与列举一致）。 */
    public long countAll() {
        long count = 0L;
        for (Drive d : driveManager.getDrives()) {
            if (!d.isOnline()) continue;
            for (String bucket : metadataIndex.bucketsOn(d)) {
                if (metadataIndex.isDegraded(bucket)) continue; // 降级桶统一在下方扫描计，避免重复/遗漏
                NavigableMap<String, ObjectMetadata> map = metadataIndex.peekIndex(d, bucket);
                if (map != null) count += map.size();
            }
        }
        for (String bucket : metadataIndex.degradedBucketsView()) {
            count += snapshot(bucket).size();
        }
        return count;
    }

    /**
     * 从磁盘重建元数据索引：扫描全部 Drive（含离线 Drive——恢复在线后无需再建）
     * 的所有 {@code object.meta}。能力启动时调用一次，成本等同于一次全量扫描。
     */
    public void rebuildIndex() {
        long start = System.currentTimeMillis();
        metadataIndex.clear();
        long total = 0L;
        for (Drive d : driveManager.getDrives()) {
            File[] bucketDirs = d.bucketsDir().listFiles();
            if (bucketDirs == null) continue;
            for (File bdir : bucketDirs) {
                if (!bdir.isDirectory()) continue;
                File objectsDir = new File(bdir, "objects");
                if (!objectsDir.isDirectory()) continue;
                total += scanInto(d, bdir.getName(), objectsDir);
            }
        }
        log.info("对象元数据索引重建完成: objects={} cost={}ms", total, System.currentTimeMillis() - start);
    }

    /** 清除指定桶的索引（删桶成功后调用），同步回收容量计数与降级标记。 */
    public void dropBucketIndex(String bucket) {
        metadataIndex.dropBucket(bucket);
    }

    /** 索引观测口（容量/降级状态供管理与测试）。 */
    public MetadataIndex metadataIndex() {
        return metadataIndex;
    }

    /** T22：索引变更代数（分区计数快照复用依据）。 */
    public long indexGeneration() {
        return metadataIndex.generation();
    }

    // ==================== 内部工具 ====================

    /** 递归扫描对象目录，将解析成功的元数据装入索引；返回装入条数。 */
    private int scanInto(Drive drive, String bucket, File dir) {
        File[] cs = dir.listFiles();
        if (cs == null) return 0;
        int count = 0;
        for (File c : cs) {
            if (c.isDirectory()) {
                count += scanInto(drive, bucket, c);
            } else if ("object.meta".equals(c.getName())) {
                try {
                    ObjectMetadata m = objectMapper.readValue(c, ObjectMetadata.class);
                    if (m != null && m.getKey() != null) {
                        metadataIndex.putDirect(drive, bucket, m.getKey(), m);
                        count++;
                    }
                } catch (Exception e) {
                    log.warn("读取对象元信息失败: {}", c, e);
                }
            }
        }
        return count;
    }

    /**
     * T3 降级目录扫描：扫描全部在线 Drive 上指定桶的对象目录，按与索引路径同口径的
     * prefix 区间（含上界）与 marker（排他）过滤后装入 sink（有序去重）。
     * 正确性优先：成本 O(桶内对象数)，仅在索引容量满后的降级桶使用。
     */
    private void collectScan(String bucket, String prefix, String upper, String marker,
                             TreeMap<String, ObjectMetadata> sink) {
        for (Drive d : driveManager.getDrives()) {
            if (!d.isOnline()) continue;
            File objectsDir = d.objectsDir(bucket);
            if (!objectsDir.isDirectory()) continue;
            scanCollect(objectsDir, prefix, upper, marker, sink);
        }
    }

    private void scanCollect(File dir, String prefix, String upper, String marker,
                             TreeMap<String, ObjectMetadata> sink) {
        File[] cs = dir.listFiles();
        if (cs == null) return;
        for (File c : cs) {
            if (c.isDirectory()) {
                scanCollect(c, prefix, upper, marker, sink);
            } else if ("object.meta".equals(c.getName())) {
                try {
                    ObjectMetadata m = objectMapper.readValue(c, ObjectMetadata.class);
                    if (m == null || m.getKey() == null) continue;
                    String key = m.getKey();
                    if (prefix != null && !key.startsWith(prefix)) continue;
                    if (upper != null && key.compareTo(upper) >= 0) continue;
                    if (marker != null && key.compareTo(marker) <= 0) continue;
                    sink.merge(key, m, (a, b) -> a.getVersion() >= b.getVersion() ? a : b);
                } catch (Exception e) {
                    log.warn("读取对象元信息失败: {}", c, e);
                }
            }
        }
    }

    /**
     * prefix 区间的排他上界：大于所有以 {@code prefix} 为前缀的字符串的最小字符串。
     * prefix 全为 {@code \uFFFF} 时不存在上界，返回 null（调用方退化为仅下界）。
     */
    private static String prefixUpperBound(String prefix) {
        for (int i = prefix.length() - 1; i >= 0; i--) {
            char c = prefix.charAt(i);
            if (c != Character.MAX_VALUE) {
                return prefix.substring(0, i) + (char) (c + 1);
            }
        }
        return null;
    }

    private ObjectMetadata readMeta(Drive drive, String bucket, String keySha1) {
        File f = drive.objectMetaFile(bucket, keySha1);
        if (!f.exists()) return null;
        try {
            return objectMapper.readValue(f, ObjectMetadata.class);
        } catch (Exception e) {
            // T15：损坏 meta ≠ 不存在——隔离 .corrupt + 告警 + 计数；调用方按显式失败处理
            // （get/head 映射 500、putPrimary 拒绝覆盖写防 version 重置），不静默当 404
            corruptedMetaQuarantined.incrementAndGet();
            log.error("T15 对象元数据损坏，已隔离: {} : {}", f, e.getMessage());
            File corrupt = new File(f.getParentFile(), f.getName() + ".corrupt."
                    + System.currentTimeMillis());
            if (!f.renameTo(corrupt)) {
                log.error("T15 损坏元数据隔离改名失败（保留原位）: {}", f);
            }
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

    /** T13：文件 fsync（失败仅告警——个别文件系统不支持）。 */
    private static void fsyncFileQuietly(File f) {
        try (FileChannel ch = FileChannel.open(f.toPath(), java.nio.file.StandardOpenOption.WRITE)) {
            ch.force(true);
        } catch (IOException e) {
            log.warn("文件 fsync 失败（忽略）: {}: {}", f, e.getMessage());
        }
    }

    /** T13：目录 fsync（Windows 不支持目录 force，异常静默忽略）。 */
    private static void fsyncDirQuietly(File dir) {
        try (FileChannel ch = FileChannel.open(dir.toPath(), java.nio.file.StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException ignored) {
            // 目录 fsync 在部分平台不可用，静默跳过
        }
    }

    /**
     * T11：崩溃残留清扫——删除各 Drive 上超期的写临时文件
     * （{@code object.*.tmp}，含 T5 备份文件 {@code object.data.old*.tmp}）。
     * 年龄阈值防误删在途写；启动时调用一次 + 周期 GC 共用。
     *
     * @return 删除的残留文件数
     */
    public int sweepTempFiles(long maxAgeMillis) {
        long deadline = System.currentTimeMillis() - Math.max(0, maxAgeMillis);
        int removed = 0;
        for (Drive d : driveManager.getDrives()) {
            File[] bucketDirs = d.bucketsDir().listFiles();
            if (bucketDirs == null) continue;
            for (File bdir : bucketDirs) {
                if (!bdir.isDirectory()) continue;
                File objectsDir = new File(bdir, "objects");
                if (!objectsDir.isDirectory()) continue;
                removed += sweepTempIn(objectsDir, deadline);
            }
        }
        if (removed > 0) {
            log.warn("T11 崩溃残留清扫：删除超期临时文件 {} 个（年龄阈值 {} ms）", removed, maxAgeMillis);
        }
        return removed;
    }

    private int sweepTempIn(File dir, long deadline) {
        int removed = 0;
        File[] cs = dir.listFiles();
        if (cs == null) return 0;
        for (File c : cs) {
            if (c.isDirectory()) {
                removed += sweepTempIn(c, deadline);
            } else if (c.getName().startsWith("object.") && c.getName().endsWith(".tmp")
                    && c.lastModified() < deadline) {
                if (c.delete()) {
                    removed++;
                }
            }
        }
        return removed;
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
