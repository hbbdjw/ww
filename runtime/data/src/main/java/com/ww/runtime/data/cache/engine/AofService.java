package com.ww.runtime.data.cache.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redis风格AOF（Append Only File）持久化服务
 *
 * <p>特性：
 * <ul>
 *   <li>二进制文件格式，带CRC32校验</li>
 *   <li>三种fsync策略：always/everysec/no</li>
 *   <li>后台AOF重写（BGREWRITEAOF），最小化命令集</li>
 *   <li>重写期间双写缓冲，保证数据不丢失</li>
 *   <li>原子文件替换，保证文件完整性</li>
 *   <li>启动时自动加载回放</li>
 * </ul>
 */
public class AofService {

    private static final Logger log = LoggerFactory.getLogger(AofService.class);

    private final AofProperties properties;
    private final SimpleCache<String, Object> cache;

    private File aofFile;
    private File aofDir;
    private OutputStream aofOut;
    private DataOutputStream aofDataOut;
    /** 穿透装饰器的原始流引用：fsync（getFD().sync()）与损坏截断（channel().truncate）需要它 */
    private FileOutputStream aofRawOut;
    private final Object writeLock = new Object();
    private long aofFileSize = 0;
    private long lastRewriteSize = 0;

    private ScheduledExecutorService scheduler;
    private volatile long lastFsyncTime = 0;
    private volatile boolean needsFsync = false;

    private volatile boolean rewriting = false;
    private final ReentrantLock rewriteLock = new ReentrantLock();
    private final List<byte[]> rewriteBuffer = new ArrayList<byte[]>();
    private Thread rewriteThread;

    private long lastFlushFailTime = 0;
    private int consecutiveFailures = 0;
    private long commandCount = 0;
    /** 回放时跳过的毒记录条数（CRC 完好但应用失败，如存量 ZSet 误写的 OP_HSET） */
    private final java.util.concurrent.atomic.AtomicLong poisonRecordsSkipped =
            new java.util.concurrent.atomic.AtomicLong();
    /** 回放 CRC 校验失败次数（结构性损坏） */
    private final java.util.concurrent.atomic.AtomicLong crcFailures =
            new java.util.concurrent.atomic.AtomicLong();
    /** 因 key 超过编码上限被拒绝落盘的写操作次数 */
    private final java.util.concurrent.atomic.AtomicLong oversizedKeysRejected =
            new java.util.concurrent.atomic.AtomicLong();

    public AofService(AofProperties properties, SimpleCache<String, Object> cache) {
        this.properties = properties;
        this.cache = cache;
    }

    public void start() throws IOException {
        if (!properties.isEnabled()) {
            log.info("AOF 持久化已关闭");
            return;
        }
        log.info("AOF 持久化已启用，fsync策略：{}，目录：{}", properties.getAppendfsync(), properties.getDir());

        aofDir = new File(properties.getDir());
        if (!aofDir.exists()) {
            aofDir.mkdirs();
        }
        aofFile = new File(aofDir, properties.getFilename());

        boolean isNewFile = !aofFile.exists() || aofFile.length() == 0;
        aofRawOut = new FileOutputStream(aofFile, true);
        aofOut = new BufferedOutputStream(aofRawOut, properties.getBufferSize());
        aofDataOut = new DataOutputStream(aofOut);

        if (isNewFile) {
            writeHeader();
            flushToDisk(true);
        } else {
            aofFileSize = aofFile.length();
        }

        String fsync = properties.getAppendfsync();
        if ("everysec".equals(fsync)) {
            scheduler = Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "aof-fsync");
                    t.setDaemon(true);
                    return t;
                }
            });
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    everysecFsync();
                }
            }, 1, 1, TimeUnit.SECONDS);
        }

        if (properties.isAutoRewrite()) {
            if (scheduler == null) {
                scheduler = Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "aof-rewrite-check");
                        t.setDaemon(true);
                        return t;
                    }
                });
            }
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    checkAutoRewrite();
                }
            }, 10, 60, TimeUnit.SECONDS);
        }

        log.info("AOF 启动完成，当前文件大小：{} bytes", aofFileSize);
    }

    private void writeHeader() throws IOException {
        aofDataOut.writeInt(AofFormat.AOF_MAGIC);
        aofDataOut.flush();
        aofFileSize += 4;
    }

    public void stop() {
        if (!properties.isEnabled()) return;
        log.info("正在停止AOF...");
        try {
            if (rewriting) {
                waitRewriteFinish();
            }
            flushToDisk(true);
            if (aofOut != null) {
                aofOut.close();
                aofOut = null;
                aofRawOut = null;
            }
        } catch (IOException e) {
            log.error("AOF关闭异常", e);
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("AOF已停止，共记录 {} 条命令", commandCount);
    }

    public void logTyped(String operation, String cacheKey, Object... args) {
        if (!properties.isEnabled()) return;

        byte opCode = AofFormat.opCode(operation);
        if (opCode == -1) {
            log.warn("未知AOF操作类型：{}", operation);
            return;
        }

        byte[] cmdBytes;
        try {
            cmdBytes = AofFormat.encodeCommand(opCode, cacheKey != null ? cacheKey : "", args);
        } catch (IOException e) {
            // 编码失败（典型为 key 超长）：重试无意义，拒绝该条记录 + 告警 + 计数。
            // 不得走 handleWriteFailure（那是写盘失败的重试通道，语义不同）
            oversizedKeysRejected.incrementAndGet();
            log.error("AOF命令编码失败，该写操作不落盘：op={}, key={}, err={}",
                    operation, cacheKey, e.toString());
            return;
        }

        try {
            writeCommand(cmdBytes);
        } catch (IOException e) {
            log.error("AOF写入失败: op={}, key={}", operation, cacheKey, e);
            handleWriteFailure(e);
        }
    }

    /**
     * key 可编码性前置校验：写动作执行前调用，key 超过 {@link AofFormat#MAX_KEY_BYTES}
     * 时抛 {@link IllegalArgumentException} 拒绝整个操作——避免"已写内存、无法落盘"
     * 的静默分歧。AOF 未启用或 key 为 null 时 no-op。
     */
    public void requireEncodableKey(String key) {
        if (!properties.isEnabled() || key == null) return;
        // 快路径：每字符按 3 字节 UTF-8 上限估算仍在限内，无需真实编码
        if (key.length() * 3L <= AofFormat.MAX_KEY_BYTES) return;
        int bytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (bytes > AofFormat.MAX_KEY_BYTES) {
            throw new IllegalArgumentException("key 超过 AOF 编码上限（" + bytes
                    + " 字节 > " + AofFormat.MAX_KEY_BYTES + " 字节），写入被拒绝");
        }
    }

    /**
     * @deprecated 文本拼接通道会把多参数压扁为单串导致回放损坏，
     * 唯一调用方（SimpleCache.exec）已切换 {@link #logTyped}，计划阶段 3 删除。
     */
    @Deprecated
    public void log(String operation, String cacheKey, String args) {
        if ("CLEAR".equals(operation)) {
            logTyped(operation, "", new Object[0]);
        } else if ("REMOVE".equals(operation) || "LPOP".equals(operation) || "RPOP".equals(operation)) {
            logTyped(operation, cacheKey, new Object[0]);
        } else {
            logTyped(operation, cacheKey, args != null ? args : "");
        }
    }

    private void writeCommand(byte[] cmdBytes) throws IOException {
        synchronized (writeLock) {
            aofDataOut.write(cmdBytes);
            aofFileSize += cmdBytes.length;
            commandCount++;

            if (rewriting) {
                synchronized (rewriteBuffer) {
                    rewriteBuffer.add(cmdBytes);
                }
            }

            String strategy = properties.getAppendfsync();
            if ("always".equals(strategy)) {
                flushToDisk(true);
            } else if ("everysec".equals(strategy)) {
                needsFsync = true;
                aofDataOut.flush();
            } else {
                aofDataOut.flush();
            }
        }
    }

    private void everysecFsync() {
        if (!needsFsync) return;
        try {
            flushToDisk(true);
            needsFsync = false;
            lastFsyncTime = System.currentTimeMillis();
        } catch (IOException e) {
            consecutiveFailures++;
            lastFlushFailTime = System.currentTimeMillis();
            log.error("AOF每秒fsync失败，连续失败次数：{}", consecutiveFailures, e);
        }
    }

    private void flushToDisk(boolean forceFsync) throws IOException {
        aofDataOut.flush();
        if (forceFsync && aofRawOut != null) {
            aofRawOut.getFD().sync();
        }
    }

    private void handleWriteFailure(IOException e) {
        consecutiveFailures++;
        lastFlushFailTime = System.currentTimeMillis();
        for (int attempt = 1; attempt <= properties.getFlushRetries(); attempt++) {
            try {
                Thread.sleep(properties.getFlushRetryBackoffMs() * attempt);
                flushToDisk(true);
                consecutiveFailures = 0;
                return;
            } catch (Exception ex) {
                log.warn("AOF写入重试 {}/{} 失败", attempt, properties.getFlushRetries(), ex);
            }
        }
        log.error("AOF写入连续 {} 次失败", properties.getFlushRetries(), e);
    }

    public void replay(Cache<String, Object> cache) {
        replay(cache, new java.util.function.Predicate<String>() {
            @Override
            public boolean test(String k) {
                return true;
            }
        }, true);
    }

    public void replay(Cache<String, Object> cache, java.util.function.Predicate<String> keyFilter) {
        replay(cache, keyFilter, false);
    }

    /**
     * @param truncateOnCorruption 结构性损坏（CRC 失配/尾部残记录）时是否把文件物理截断到
     *                             最后一条完好记录边界。不截断则后续追加写在损坏尾部之后，
     *                             新记录永远无法通过回放边界，等于持续静默丢数据。
     *                             <p>仅全量回放路径允许：调用前提为文件已由 {@link #start()}
     *                             打开且无并发写（截断经同一裸流 channel 完成，之后追加写
     *                             自然落在新边界）。谓词回放只覆盖部分 key，不得动文件。
     */
    private void replay(Cache<String, Object> cache, java.util.function.Predicate<String> keyFilter,
                        boolean truncateOnCorruption) {
        if (!properties.isEnabled()) return;
        if (!aofFile.exists() || aofFile.length() <= 4) {
            log.info("AOF回放完成：无日志记录");
            return;
        }

        log.info("开始回放AOF日志...");
        int applied = 0;
        int filtered = 0;
        int corrupted = 0;
        long lastGoodOffset = 0;
        boolean structuralCorruption = false;
        String corruptionReason = null;

        try (AofFormat.Reader reader = new AofFormat.Reader(
                new BufferedInputStream(new FileInputStream(aofFile), 65536))) {
            int magic = reader.readMagic();
            if (magic != AofFormat.AOF_MAGIC) {
                log.error("AOF文件魔数不匹配，期望 {:x}，实际 {:x}", AofFormat.AOF_MAGIC, magic);
                return;
            }
            lastGoodOffset = reader.bytesRead();

            while (reader.available() > 0) {
                AofFormat.Record record;
                try {
                    record = reader.readRecord();
                } catch (java.io.EOFException eof) {
                    // 尾部残记录（崩溃写一半）：后续记录边界不可信，停止回放
                    corrupted++;
                    structuralCorruption = true;
                    crcFailures.incrementAndGet();
                    corruptionReason = "尾部残记录";
                    log.error("AOF尾部残记录（位置 {}），停止回放并截断", applied + filtered);
                    break;
                } catch (AofFormat.AofFormatException bad) {
                    // 结构性损坏（含 CRC 失配）：与单条毒记录区分，不得跳过续读
                    corrupted++;
                    structuralCorruption = true;
                    crcFailures.incrementAndGet();
                    corruptionReason = bad.getMessage();
                    log.error("AOF回放结构性损坏（位置 {}）：{}", applied + filtered, bad.getMessage());
                    break;
                }
                lastGoodOffset = reader.bytesRead();

                if (keyFilter != null && !keyFilter.test(record.key())) {
                    filtered++;
                    continue;
                }

                try {
                    replayCommand(cache, record.opCode(), record.key(), record.args());
                    applied++;
                } catch (RuntimeException poison) {
                    // CRC 完好的单条毒记录（如存量 ZSet 被误写为 OP_HSET）：
                    // 跳过 + 告警 + 计数，后续记录边界仍可信，不中断回放
                    poisonRecordsSkipped.incrementAndGet();
                    log.error("AOF毒记录跳过：op={}, key={}, err={}",
                            AofFormat.opName(record.opCode()), record.key(), poison.toString());
                }
            }
        } catch (IOException e) {
            log.error("AOF文件读取失败", e);
        }

        if (structuralCorruption && truncateOnCorruption && aofRawOut != null) {
            try {
                long before = aofFile.length();
                aofRawOut.getChannel().truncate(lastGoodOffset);
                aofFileSize = lastGoodOffset;
                log.error("AOF已截断至最后完好记录边界：{} -> {} 字节（丢弃 {} 字节，原因：{}）",
                        before, lastGoodOffset, before - lastGoodOffset, corruptionReason);
            } catch (IOException e) {
                log.error("AOF截断失败", e);
            }
        }

        log.info("AOF回放完成：应用 {} 条，跳过 {} 条非owner，损坏 {} 条，毒记录跳过 {} 条",
                applied, filtered, corrupted, poisonRecordsSkipped.get());
    }

    private void replayCommand(Cache<String, Object> cache, byte opCode, String key, Object[] args) {
        SimpleCache<String, Object> simple = null;
        if (cache instanceof SimpleCache) {
            simple = (SimpleCache<String, Object>) cache;
        }

        switch (opCode) {
            case AofFormat.OP_PUT:
                cache.put(key, args[0]);
                break;
            case AofFormat.OP_PUT_EXPIRED: {
                Object v = args[0];
                long ttl = args[1] instanceof Number ? ((Number) args[1]).longValue() : 0L;
                cache.put(key, v, ttl);
                break;
            }
            case AofFormat.OP_REMOVE:
                cache.remove(key);
                break;
            case AofFormat.OP_CLEAR:
                cache.clear();
                break;
            case AofFormat.OP_HSET:
                cache.hset(key, (String) args[0], args[1]);
                break;
            case AofFormat.OP_LPUSH: {
                Object firstArg = args[0];
                if (firstArg instanceof List) {
                    List<?> list = (List<?>) firstArg;
                    cache.lpush(key, list.toArray());
                } else if (firstArg instanceof Object[]) {
                    cache.lpush(key, (Object[]) firstArg);
                } else {
                    cache.lpush(key, firstArg);
                }
                break;
            }
            case AofFormat.OP_RPUSH: {
                Object firstArg = args[0];
                if (firstArg instanceof List) {
                    List<?> list = (List<?>) firstArg;
                    cache.rpush(key, list.toArray());
                } else if (firstArg instanceof Object[]) {
                    cache.rpush(key, (Object[]) firstArg);
                } else {
                    cache.rpush(key, firstArg);
                }
                break;
            }
            case AofFormat.OP_LPOP:
                cache.lpop(key);
                break;
            case AofFormat.OP_RPOP:
                cache.rpop(key);
                break;
            case AofFormat.OP_SADD: {
                Object firstArg = args[0];
                if (firstArg instanceof List) {
                    List<?> list = (List<?>) firstArg;
                    cache.sadd(key, list.toArray());
                } else if (firstArg instanceof Object[]) {
                    cache.sadd(key, (Object[]) firstArg);
                } else {
                    cache.sadd(key, firstArg);
                }
                break;
            }
            case AofFormat.OP_SREM: {
                Object firstArg = args[0];
                if (firstArg instanceof List) {
                    List<?> list = (List<?>) firstArg;
                    cache.srem(key, list.toArray());
                } else if (firstArg instanceof Object[]) {
                    cache.srem(key, (Object[]) firstArg);
                } else {
                    cache.srem(key, firstArg);
                }
                break;
            }
            case AofFormat.OP_ZADD: {
                double score = args[0] instanceof Number ? ((Number) args[0]).doubleValue() : 0.0;
                cache.zadd(key, score, args[1]);
                break;
            }
            case AofFormat.OP_XADD: {
                long idMs = ((Number) args[0]).longValue();
                long idSeq = ((Number) args[1]).longValue();
                Map<String, String> fields = (Map<String, String>) args[2];
                if (simple != null) {
                    simple.xaddRaw(key, new StreamId(idMs, idSeq), fields);
                }
                break;
            }
            case AofFormat.OP_XDEL: {
                if (args[0] instanceof List && simple != null) {
                    List<?> idsList = (List<?>) args[0];
                    List<StreamId> ids = new ArrayList<StreamId>();
                    for (int i = 0; i + 1 < idsList.size(); i += 2) {
                        long ms = ((Number) idsList.get(i)).longValue();
                        long sq = ((Number) idsList.get(i + 1)).longValue();
                        ids.add(new StreamId(ms, sq));
                    }
                    simple.getStreamStore().xdel(key, ids.toArray(new StreamId[0]));
                }
                break;
            }
            case AofFormat.OP_XTRIM: {
                long maxLen = ((Number) args[0]).longValue();
                boolean approximate = (Boolean) args[1];
                if (simple != null) {
                    simple.getStreamStore().xtrim(key, maxLen, approximate);
                }
                break;
            }
            case AofFormat.OP_XGROUP_CREATE: {
                String groupName = (String) args[0];
                long startMs = ((Number) args[1]).longValue();
                long startSeq = ((Number) args[2]).longValue();
                boolean mkstream = (Boolean) args[3];
                if (simple != null) {
                    simple.xgroupCreateRaw(key, groupName, new StreamId(startMs, startSeq), mkstream);
                }
                break;
            }
            case AofFormat.OP_XACK: {
                String groupName = (String) args[0];
                if (args[1] instanceof List && simple != null) {
                    List<?> idsList = (List<?>) args[1];
                    List<Long> flat = new ArrayList<Long>();
                    for (Object o : idsList) {
                        if (o instanceof Number) {
                            flat.add(((Number) o).longValue());
                        }
                    }
                    simple.xackRaw(key, groupName, flat);
                }
                break;
            }
            case AofFormat.OP_XGROUP_DESTROY: {
                String groupName = (String) args[0];
                if (simple != null) {
                    simple.xgroupDestroyRaw(key, groupName);
                }
                break;
            }
            case AofFormat.OP_XGROUP_CREATECONSUMER: {
                String groupName = (String) args[0];
                String consumerName = (String) args[1];
                if (simple != null) {
                    simple.xgroupCreateConsumerRaw(key, groupName, consumerName);
                }
                break;
            }
            case AofFormat.OP_XGROUP_DELCONSUMER: {
                String groupName = (String) args[0];
                String consumerName = (String) args[1];
                if (simple != null) {
                    simple.xgroupDelConsumerRaw(key, groupName, consumerName);
                }
                break;
            }
            case AofFormat.OP_XPEL_ENTRY: {
                // 回放：恢复单条 PEL 条目（key, groupName, consumerName, idMs, idSeq）
                String groupName = (String) args[0];
                String consumerName = (String) args[1];
                long idMs = ((Number) args[2]).longValue();
                long idSeq = ((Number) args[3]).longValue();
                if (simple != null) {
                    simple.getStreamStore().restorePel(key, groupName, consumerName, new StreamId(idMs, idSeq));
                }
                break;
            }
            case AofFormat.OP_XCLAIM: {
                String groupName = (String) args[0];
                String consumerName = (String) args[1];
                if (args[2] instanceof List && simple != null) {
                    List<?> idsList = (List<?>) args[2];
                    List<Long> flat = new ArrayList<Long>();
                    for (Object o : idsList) {
                        if (o instanceof Number) {
                            flat.add(((Number) o).longValue());
                        }
                    }
                    simple.xclaimRaw(key, groupName, consumerName, flat);
                }
                break;
            }
            case AofFormat.OP_XDELIVER: {
                String groupName = (String) args[0];
                String consumerName = (String) args[1];
                long lastMs = ((Number) args[2]).longValue();
                long lastSeq = ((Number) args[3]).longValue();
                if (simple != null) {
                    List<Long> flat = new ArrayList<Long>();
                    if (args[4] instanceof List) {
                        List<?> idsList = (List<?>) args[4];
                        for (Object o : idsList) {
                            if (o instanceof Number) {
                                flat.add(((Number) o).longValue());
                            }
                        }
                    }
                    simple.xdeliverRaw(key, groupName, consumerName, lastMs, lastSeq, flat);
                }
                break;
            }
            default:
                log.warn("未知AOF操作类型：{}", AofFormat.opName(opCode));
        }
    }

    public void bgRewriteAof() {
        if (!properties.isEnabled()) {
            log.info("AOF未启用，跳过重写");
            return;
        }
        if (rewriting) {
            log.info("AOF重写已在进行中");
            return;
        }
        startRewrite();
    }

    private void checkAutoRewrite() {
        if (rewriting) return;
        long minSize = properties.getAutoRewriteMinSizeMb() * 1024 * 1024;
        if (aofFileSize < minSize) return;

        long baseSize = lastRewriteSize > 0 ? lastRewriteSize : minSize;
        long growth = aofFileSize - baseSize;
        int percentage = (int) (growth * 100 / baseSize);
        if (percentage >= properties.getAutoRewritePercentage()) {
            log.info("触发AOF自动重写：当前大小 {} MB，基准 {} MB，增长 {}%",
                    aofFileSize / 1024 / 1024, baseSize / 1024 / 1024, percentage);
            startRewrite();
        }
    }

    private void startRewrite() {
        rewriteLock.lock();
        try {
            rewriting = true;
            synchronized (rewriteBuffer) {
                rewriteBuffer.clear();
            }
            rewriteThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    doRewrite();
                }
            }, "aof-rewrite");
            rewriteThread.setDaemon(true);
            rewriteThread.start();
        } finally {
            rewriteLock.unlock();
        }
    }

    private void doRewrite() {
        File tmpFile = new File(aofDir, properties.getFilename() + AofFormat.REWRITE_TMP_SUFFIX);
        log.info("开始AOF后台重写，临时文件：{}", tmpFile.getAbsolutePath());
        long startTime = System.currentTimeMillis();

        FileOutputStream tmpFos = null;
        try (BufferedOutputStream bos = new BufferedOutputStream(
                    (tmpFos = new FileOutputStream(tmpFile)), properties.getBufferSize() * 2);
             DataOutputStream out = new DataOutputStream(bos)) {

            out.writeInt(AofFormat.AOF_MAGIC);

            int kvCount = writeKeyValueEntries(out);
            int hashCount = writeHashEntries(out);
            int listCount = writeListEntries(out);
            int setCount = writeSetEntries(out);
            int zsetCount = writeZSetEntries(out);
            int streamCount = writeStreamEntries(out);

            int buffered;
            synchronized (rewriteBuffer) {
                for (byte[] cmd : rewriteBuffer) {
                    out.write(cmd);
                }
                buffered = rewriteBuffer.size();
                rewriteBuffer.clear();
            }

            out.flush();
            tmpFos.getFD().sync();
            long newSize = tmpFile.length();

            // 缓冲排空与 fsync 在 writeLock 之外执行，写锁只包含换文件动作
            synchronized (rewriteBuffer) {
                for (byte[] cmd : rewriteBuffer) {
                    out.write(cmd);
                }
                buffered += rewriteBuffer.size();
                rewriteBuffer.clear();
            }
            out.flush();
            tmpFos.getFD().sync();

            // 换文件前关闭临时文件流：Windows 上 Files.move 要求源文件无打开句柄，
            // 否则 move 抛 FileSystemException（POSIX 无此限制）。
            // 关闭后 try-with-resources 的重复 close 为空操作（缓冲已排空、FOS close 幂等）。
            out.close();

            synchronized (writeLock) {
                // 关闭旧主 AOF 流（Windows 同样要求目标无打开句柄）
                aofDataOut.close();
                try {
                    Files.move(tmpFile.toPath(), aofFile.toPath(), StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException moveFail) {
                    // move 失败：旧文件未被替换仍然完好，重开续写，避免旧流已关闭导致
                    // 后续写入静默落空（缓冲写入不抛异常、flush 异常被上层吞掉）
                    log.error("AOF重写换文件失败，重开旧文件续写", moveFail);
                    aofRawOut = new FileOutputStream(aofFile, true);
                    aofOut = new BufferedOutputStream(aofRawOut, properties.getBufferSize());
                    aofDataOut = new DataOutputStream(aofOut);
                    throw moveFail;
                }
                aofRawOut = new FileOutputStream(aofFile, true);
                aofOut = new BufferedOutputStream(aofRawOut, properties.getBufferSize());
                aofDataOut = new DataOutputStream(aofOut);
                aofFileSize = aofFile.length();
                lastRewriteSize = aofFileSize;
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("AOF重写完成：KV {} 条，Hash {} 条，List {} 条，Set {} 条，ZSet {} 条，Stream {} 条，" +
                            "缓冲 {} 条，新文件大小 {} MB，耗时 {} ms",
                    kvCount, hashCount, listCount, setCount, zsetCount, streamCount,
                    buffered, newSize / 1024 / 1024, duration);

        } catch (Exception e) {
            log.error("AOF重写失败", e);
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        } finally {
            rewriting = false;
            rewriteThread = null;
        }
    }

    private int writeKeyValueEntries(DataOutputStream out) throws IOException {
        int count = 0;
        for (Map.Entry<String, CacheEntry<Object>> e : cache.getAllKeyValueEntries()) {
            String key = e.getKey();
            CacheEntry<Object> entry = e.getValue();
            if (entry.isExpired()) continue;
            Object value = entry.getValue();

            byte[] cmd;
            if (entry.getExpireTime() == Long.MAX_VALUE) {
                cmd = AofFormat.encodeCommand(AofFormat.OP_PUT, key, value);
            } else {
                long remainingTtl = entry.getExpireTime() - System.currentTimeMillis();
                if (remainingTtl <= 0) continue;
                cmd = AofFormat.encodeCommand(AofFormat.OP_PUT_EXPIRED, key, value, remainingTtl);
            }
            out.write(cmd);
            count++;
        }
        return count;
    }

    @SuppressWarnings("unchecked")
    private int writeHashEntries(DataOutputStream out) throws IOException {
        int count = 0;
        for (Map.Entry<String, Map<String, Object>> e : cache.getAllHashEntries()) {
            String key = e.getKey();
            Map<String, Object> hash = e.getValue();
            for (Map.Entry<String, Object> fe : hash.entrySet()) {
                byte[] cmd = AofFormat.encodeCommand(AofFormat.OP_HSET, key, fe.getKey(), fe.getValue());
                out.write(cmd);
                count++;
            }
        }
        return count;
    }

    @SuppressWarnings("unchecked")
    private int writeListEntries(DataOutputStream out) throws IOException {
        int count = 0;
        for (Map.Entry<String, ConcurrentLinkedDeque<Object>> e : cache.getAllListEntries()) {
            String key = e.getKey();
            ConcurrentLinkedDeque<Object> deque = e.getValue();
            Object[] arr = deque.toArray();
            if (arr.length > 0) {
                byte[] cmd = AofFormat.encodeCommand(AofFormat.OP_RPUSH, key, new ArrayList<Object>(java.util.Arrays.asList(arr)));
                out.write(cmd);
                count++;
            }
        }
        return count;
    }

    @SuppressWarnings("unchecked")
    private int writeSetEntries(DataOutputStream out) throws IOException {
        int count = 0;
        for (Map.Entry<String, Set<Object>> e : cache.getAllSetEntries()) {
            String key = e.getKey();
            Set<Object> set = e.getValue();
            if (!set.isEmpty()) {
                byte[] cmd = AofFormat.encodeCommand(AofFormat.OP_SADD, key, new ArrayList<Object>(set));
                out.write(cmd);
                count++;
            }
        }
        return count;
    }

    @SuppressWarnings("unchecked")
    private int writeZSetEntries(DataOutputStream out) throws IOException {
        int count = 0;
        for (Map.Entry<String, ConcurrentSkipListMap<Double, Set<Object>>> e : cache.getAllZSetEntries()) {
            String key = e.getKey();
            ConcurrentSkipListMap<Double, Set<Object>> zset = e.getValue();
            for (Map.Entry<Double, Set<Object>> scoreEntry : zset.entrySet()) {
                double score = scoreEntry.getKey();
                for (Object member : scoreEntry.getValue()) {
                    byte[] cmd = AofFormat.encodeCommand(AofFormat.OP_ZADD, key, score, member);
                    out.write(cmd);
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * 写 Stream 条目到 AOF 重写文件。
     * <p>除 stream entries 与 group 创建外，还持久化每个 consumer 及其 PEL
     * （pending entries list），使重启后消费者组状态可完整恢复。
     * <p>写入顺序：XADD（消息体）→ XGROUP_CREATE（组）→ XGROUP_CREATECONSUMER（消费者）
     * → XPEL_ENTRY（每条 PEL），回放时按此顺序重建完整状态。
     */
    private int writeStreamEntries(DataOutputStream out) throws IOException {
        int count = 0;
        StreamStore streamStore = cache.getStreamStore();
        for (String streamKey : streamStore.streamKeys()) {
            for (Map.Entry<StreamId, StreamEntry> entry : streamStore.getStreamEntries(streamKey)) {
                StreamId id = entry.getKey();
                Map<String, String> fields = entry.getValue().getFields();
                byte[] cmd = AofFormat.encodeCommand(AofFormat.OP_XADD, streamKey, id.getMsTime(), id.getSeq(), fields);
                out.write(cmd);
                count++;
            }

            for (Map.Entry<String, ConsumerGroup> ge : streamStore.getStreamGroups(streamKey).entrySet()) {
                String groupName = ge.getKey();
                ConsumerGroup group = ge.getValue();
                StreamId lastDelivered = group.getLastDeliveredId();
                byte[] cmd = AofFormat.encodeCommand(AofFormat.OP_XGROUP_CREATE, streamKey, groupName,
                        lastDelivered.getMsTime(), lastDelivered.getSeq(), true);
                out.write(cmd);
                count++;

                // 持久化每个消费者及其 PEL 条目
                for (Map.Entry<String, Consumer> ce : group.getConsumers().entrySet()) {
                    String consumerName = ce.getKey();
                    byte[] createConsumer = AofFormat.encodeCommand(AofFormat.OP_XGROUP_CREATECONSUMER,
                            streamKey, groupName, consumerName);
                    out.write(createConsumer);
                    count++;
                    // 写该消费者持有的每条 PEL 条目（已投递未 XACK 的消息）
                    for (StreamId pelId : ce.getValue().pendingIdsView()) {
                        byte[] pelCmd = AofFormat.encodeCommand(AofFormat.OP_XPEL_ENTRY, streamKey,
                                groupName, consumerName, pelId.getMsTime(), pelId.getSeq());
                        out.write(pelCmd);
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private void waitRewriteFinish() {
        if (rewriteThread != null && rewriteThread.isAlive()) {
            try {
                rewriteThread.join(30000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public AofStatus getStatus() {
        synchronized (writeLock) {
            return new AofStatus(lastFlushFailTime, consecutiveFailures,
                    rewriting ? rewriteBuffer.size() : 0, aofFileSize,
                    rewriting, commandCount, poisonRecordsSkipped.get(), crcFailures.get(),
                    oversizedKeysRejected.get());
        }
    }

    public static class AofStatus {
        private final long lastFlushFailTime;
        private final int consecutiveFailures;
        private final int bufferSize;
        private final long fileSizeBytes;
        private final boolean rewriting;
        private final long totalCommands;
        private final long poisonRecordsSkipped;
        private final long crcFailures;
        private final long oversizedKeysRejected;

        public AofStatus(long lastFlushFailTime, int consecutiveFailures, int bufferSize,
                         long fileSizeBytes, boolean rewriting, long totalCommands,
                         long poisonRecordsSkipped, long crcFailures, long oversizedKeysRejected) {
            this.lastFlushFailTime = lastFlushFailTime;
            this.consecutiveFailures = consecutiveFailures;
            this.bufferSize = bufferSize;
            this.fileSizeBytes = fileSizeBytes;
            this.rewriting = rewriting;
            this.totalCommands = totalCommands;
            this.poisonRecordsSkipped = poisonRecordsSkipped;
            this.crcFailures = crcFailures;
            this.oversizedKeysRejected = oversizedKeysRejected;
        }

        public long getLastFlushFailTime() { return lastFlushFailTime; }
        public int getConsecutiveFailures() { return consecutiveFailures; }
        public int getBufferSize() { return bufferSize; }
        public long getFileSizeBytes() { return fileSizeBytes; }
        public boolean isRewriting() { return rewriting; }
        public long getTotalCommands() { return totalCommands; }
        /** 回放跳过的毒记录条数（CRC 完好但应用失败） */
        public long getPoisonRecordsSkipped() { return poisonRecordsSkipped; }
        /** 回放 CRC 校验失败次数（结构性损坏） */
        public long getCrcFailures() { return crcFailures; }
        /** 因 key 超过编码上限被拒绝落盘的写操作次数 */
        public long getOversizedKeysRejected() { return oversizedKeysRejected; }
    }
}
