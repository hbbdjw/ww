package com.ww.runtime.data.storage;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 迁移导入暂存聚合器（T1）：按 (bucket\0key) 把导入分片流式追加到暂存文件，
 * 消灭旧实现 {@code ByteArrayOutputStream} 的整对象堆内聚合与 fin 时的双倍拷贝。
 *
 * <p>守护三件套：
 * <ol>
 *   <li>并发聚合数上限：超限拒绝新建聚合 + 告警计数（背压传导至迁移引擎重试）；</li>
 *   <li>单聚合字节上限（= maxObjectSize，与 T12 联动）：超限即中止清聚合；</li>
 *   <li>超时清扫：流中断无 fin 片的暂存超期删除——覆盖"流中断无 fin"泄漏路径；
 *       启动时先清扫暂存目录（重启残留）。</li>
 * </ol>
 *
 * <p>偏移连续性语义与旧实现一致：乱序/残缺分片即中止该聚合并返回失败，
 * 迁移引擎自重发自对象首片起可完整恢复（checkpoint 对齐对象边界）。
 */
public final class ImportAggregator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ImportAggregator.class);

    private static final String STAGING_SUFFIX = ".mig";

    private final File stagingDir;
    private final int maxAggregates;
    private final long maxBytesPerAggregate;
    private final long timeoutMillis;

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    /** 因并发聚合数超限被拒绝的片数 */
    private final AtomicLong rejectedOnConcurrency = new AtomicLong();
    /** 因单聚合字节超限被中止的聚合数 */
    private final AtomicLong abortedOnOverflow = new AtomicLong();
    /** 超时清扫删除的暂存数（含启动清扫） */
    private final AtomicLong staleSwept = new AtomicLong();

    private ScheduledExecutorService sweeper;
    private volatile boolean closed;

    /** 单个对象的聚合会话：暂存文件通道 + 累计字节 + 最近活动时间。 */
    private static final class Session {
        final FileChannel channel;
        final File file;
        long bytes;
        volatile long lastActivityAt;

        Session(FileChannel channel, File file) {
            this.channel = channel;
            this.file = file;
            this.lastActivityAt = System.currentTimeMillis();
        }
    }

    /**
     * @param stagingDir           暂存目录（不存在则创建；启动时清扫其中全部暂存文件）
     * @param maxAggregates        并发聚合数上限
     * @param maxBytesPerAggregate 单聚合字节上限（= 对象大小上限）
     * @param timeoutMinutes       未完成聚合超时分钟数（&lt;=0 关闭周期清扫，启动清扫仍执行）
     */
    public ImportAggregator(File stagingDir, int maxAggregates,
                            long maxBytesPerAggregate, int timeoutMinutes) {
        if (maxAggregates <= 0) {
            throw new IllegalArgumentException("maxAggregates must be > 0, got " + maxAggregates);
        }
        this.stagingDir = stagingDir;
        this.maxAggregates = maxAggregates;
        this.maxBytesPerAggregate = maxBytesPerAggregate;
        this.timeoutMillis = TimeUnit.MINUTES.toMillis(Math.max(1, timeoutMinutes));
    }

    /** 建暂存目录 + 清扫重启残留 + 启动周期清扫守护。 */
    public void start() {
        if (!stagingDir.exists() && !stagingDir.mkdirs()) {
            throw new UncheckedIOException(new IOException(
                    "无法创建迁移导入暂存目录: " + stagingDir));
        }
        // 重启残留清扫：上一进程未完成的暂存一律作废（迁移自源端断点续跑恢复）
        File[] stale = stagingDir.listFiles((d, name) -> name.endsWith(STAGING_SUFFIX));
        if (stale != null) {
            for (File f : stale) {
                if (f.delete()) {
                    staleSwept.incrementAndGet();
                }
            }
            if (stale.length > 0) {
                LOG.warn("迁移导入暂存启动清扫：删除重启残留暂存文件 {} 个", stale.length);
            }
        }
        if (timeoutMillis > 0) {
            sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "import-staging-sweeper");
                t.setDaemon(true);
                return t;
            });
            sweeper.scheduleAtFixedRate(this::sweepStaleQuietly, 1, 1, TimeUnit.MINUTES);
        }
    }

    /**
     * 追加一片数据。首片（offset=0）创建暂存；后续片按 {@code offset == 已写字节数}
     * 校验连续性。失败（乱序/残缺/超限/并发上限）即中止该聚合并返回 false。
     */
    public boolean append(String aggKey, byte[] data, long offset) {
        if (closed) {
            return false;
        }
        Session session = sessions.get(aggKey);
        if (session == null) {
            if (offset != 0) {
                return false; // 乱序/残缺：无既有聚合可比对，拒绝（完整重喂可恢复）
            }
            if (sessions.size() >= maxAggregates) {
                rejectedOnConcurrency.incrementAndGet();
                LOG.error("迁移导入暂存并发聚合超限（>= {}），拒绝新聚合：{}", maxAggregates, aggKey);
                return false;
            }
            Session created = createSession(aggKey);
            if (created == null) {
                return false;
            }
            Session raced = sessions.putIfAbsent(aggKey, created);
            if (raced != null) {
                closeQuietly(created); // 并发建会话竞争：丢弃自己这份，复用既有
                session = raced;
            } else {
                session = created;
            }
        }
        synchronized (session) {
            session.lastActivityAt = System.currentTimeMillis();
            if (offset != session.bytes) {
                LOG.warn("迁移导入偏移不连续（残缺/乱序），中止聚合：{}（期望 {} 实际 {}）",
                        aggKey, session.bytes, offset);
                abortLocked(aggKey, session);
                return false;
            }
            // T12 联动：聚合累计上限，超限即中止清聚合
            if (session.bytes + data.length > maxBytesPerAggregate) {
                abortedOnOverflow.incrementAndGet();
                LOG.warn("迁移导入聚合超出对象大小上限，中止：{}（累计 {} > {}）",
                        aggKey, session.bytes + data.length, maxBytesPerAggregate);
                abortLocked(aggKey, session);
                return false;
            }
            try {
                // 位置参数写（通道位置不变）：恒在当前累计字节处追加，与偏移连续性校验同口径
                ByteBuffer buf = ByteBuffer.wrap(data);
                while (buf.hasRemaining()) {
                    session.channel.write(buf, session.bytes);
                }
            } catch (IOException e) {
                LOG.error("迁移导入暂存写入失败，中止聚合：{}", aggKey, e);
                abortLocked(aggKey, session);
                return false;
            }
            session.bytes += data.length;
            return true;
        }
    }

    /**
     * 完成聚合：校验累计字节 == 期望大小，返回暂存文件（调用方流式消费后自行删除）；
     * 尺寸不符/聚合缺失即中止并返回 null。
     */
    public File finish(String aggKey, long expectedSize) {
        Session session = sessions.remove(aggKey);
        if (session == null) {
            return null;
        }
        synchronized (session) {
            try {
                session.channel.close();
            } catch (IOException e) {
                LOG.warn("迁移导入暂存关闭失败：{}", aggKey, e);
            }
            if (session.bytes != expectedSize) {
                LOG.warn("迁移导入尺寸与元数据不符，中止：{}（累计 {} != 期望 {}）",
                        aggKey, session.bytes, expectedSize);
                if (session.file.exists()) {
                    session.file.delete();
                }
                return null;
            }
            return session.file;
        }
    }

    /** 中止聚合（失败路径清理）：幂等。 */
    public void abort(String aggKey) {
        Session session = sessions.remove(aggKey);
        if (session != null) {
            synchronized (session) {
                abortLocked(aggKey, session);
            }
        }
    }

    /** 清扫超期未完成聚合（覆盖"流中断无 fin"泄漏路径）；供守护线程与测试直调。 */
    public void sweepStale() {
        sweepStale(timeoutMillis);
    }

    /** 可指定超时阈值的清扫（包内可见，供测试免去分钟级等待）。 */
    void sweepStale(long timeoutMsOverride) {
        long deadline = System.currentTimeMillis() - timeoutMsOverride;
        for (Map.Entry<String, Session> e : sessions.entrySet()) {
            Session session = e.getValue();
            if (session.lastActivityAt < deadline) {
                if (sessions.remove(e.getKey(), session)) {
                    LOG.warn("迁移导入暂存超时清扫：{}（超过 {} ms 无活动）",
                            e.getKey(), timeoutMillis);
                    synchronized (session) {
                        abortLocked(e.getKey(), session);
                    }
                    staleSwept.incrementAndGet();
                }
            }
        }
    }

    /** 停机收口：停守护 + 中止全部在途聚合（迁移自源端断点续跑恢复）。 */
    @Override
    public void close() {
        closed = true;
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
        for (String aggKey : sessions.keySet()) {
            abort(aggKey);
        }
    }

    public long getRejectedOnConcurrency() { return rejectedOnConcurrency.get(); }
    public long getAbortedOnOverflow() { return abortedOnOverflow.get(); }
    public long getStaleSwept() { return staleSwept.get(); }
    /** 在途聚合数（测试/管理观测）。 */
    public int pendingCount() { return sessions.size(); }
    public File getStagingDir() { return stagingDir; }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    private void sweepStaleQuietly() {
        try {
            sweepStale();
        } catch (RuntimeException e) {
            LOG.error("迁移导入暂存清扫异常", e);
        }
    }

    private Session createSession(String aggKey) {
        File file = new File(stagingDir, stageName(aggKey));
        try {
            FileChannel channel = FileChannel.open(file.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            return new Session(channel, file);
        } catch (IOException e) {
            LOG.error("迁移导入暂存创建失败：{}", aggKey, e);
            return null;
        }
    }

    /** 暂存文件名 = SHA-1(aggKey) 十六进制（桶/键可含任意字符，不可直接作文件名）。 */
    private static String stageName(String aggKey) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] digest = sha1.digest(aggKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return hex + STAGING_SUFFIX;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 不可用", e);
        }
    }

    /** 锁内中止：关通道 + 删文件（会话须已从 sessions 移除或即将移除）。 */
    private void abortLocked(String aggKey, Session session) {
        sessions.remove(aggKey);
        closeQuietly(session);
    }

    private static void closeQuietly(Session session) {
        try {
            session.channel.close();
        } catch (IOException ignored) {
            // 关闭失败不阻断中止流程
        }
        if (session.file.exists()) {
            session.file.delete();
        }
    }
}
