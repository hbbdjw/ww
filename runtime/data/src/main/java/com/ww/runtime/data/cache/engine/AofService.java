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
    /**
     * 换文件窗口期专属缓冲（C3）：swapping=true 后到达的命令只存此处（旧流已冻结），
     * 与 rewriteBuffer 分离使换文件失败时可无重复地回放至旧流（rewriteBuffer 内命令旧流已有副本，
     * 回放会造成 LPUSH 类命令重复应用）。
     */
    private final List<byte[]> swapPending = new ArrayList<byte[]>();
    /** 双写缓冲累计字节（C12 上限约束，超限中止本次重写） */
    private final java.util.concurrent.atomic.AtomicLong rewriteBufferBytes =
            new java.util.concurrent.atomic.AtomicLong();
    /**
     * 换文件进行中（C3）：置位后写路径命令只进缓冲、绝不写旧流，
     * 排空+换文件在同一段 writeLock 临界区完成，消灭旧流被替换前的丢写窗口。
     */
    private volatile boolean swapping = false;
    /** 缓冲超限中止标志：重写线程承接中止逻辑（复位状态+删 tmp，缓冲留存待下轮） */
    private volatile boolean rewriteAbort = false;
    /** 停机保护位：stop() 等待重写超时后置位，禁止换文件避免与关流竞态（C36） */
    private volatile boolean forbidSwap = false;
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
    /** 重写因双写缓冲超限被中止的次数（C12） */
    private final java.util.concurrent.atomic.AtomicLong rewriteAbortedOnOverflow =
            new java.util.concurrent.atomic.AtomicLong();
    /** 编码异常被拒绝落盘的写操作次数（C19：不向业务抛出，避免误判失败重试导致重复数据） */
    private final java.util.concurrent.atomic.AtomicLong encodeFailuresRejected =
            new java.util.concurrent.atomic.AtomicLong();
    /**
     * 当前 AOF 文件格式版本（§3.4）：v1=存量文件（相对 TTL、无 EXPIRE、short 字符串长度），
     * v2=新文件/重写产物（绝对 TTL、OP_EXPIRE、int 长度）。写路径按此分支，重写成功后升级为 v2。
     */
    private volatile int formatVersion = AofFormat.FORMAT_VERSION_LATEST;

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
        if (isNewFile) {
            // 新文件直接使用当前版本（v2），无存量兼容负担
            formatVersion = AofFormat.FORMAT_VERSION_LATEST;
        } else {
            // 存量文件：按文件头魔数判定版本（v1 走兼容写路径，首次重写后升级 v2）
            formatVersion = detectFormatVersion();
        }
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

    /** 探测存量文件格式版本：v1 magic=4 字节；v2=magic+版本号。未知魔数按 v1 处理（回放时报错）。 */
    private int detectFormatVersion() {
        try (java.io.DataInputStream in = new java.io.DataInputStream(
                new BufferedInputStream(new java.io.FileInputStream(aofFile)))) {
            int magic = in.readInt();
            if (magic == AofFormat.AOF_MAGIC_V2) {
                return Math.max(AofFormat.FORMAT_VERSION_V2, in.readInt());
            }
            return AofFormat.FORMAT_VERSION_V1;
        } catch (IOException e) {
            log.warn("AOF 格式版本探测失败，按 v1 处理：{}", e.toString());
            return AofFormat.FORMAT_VERSION_V1;
        }
    }

    private void writeHeader() throws IOException {
        AofFormat.writeHeader(aofDataOut, formatVersion);
        aofDataOut.flush();
        aofFileSize += formatVersion >= AofFormat.FORMAT_VERSION_V2 ? 8 : 4;
    }

    public void stop() {
        if (!properties.isEnabled()) return;
        log.info("正在停止AOF...");
        try {
            if (rewriting && !waitRewriteFinish()) {
                // C36：等待超时仍在途——置位禁止换文件，避免关流与换文件竞态；
                // 重写线程将在换文件前置检查处中止（若已进入临界区则很快自行完成）
                forbidSwap = true;
                log.error("AOF停止时重写未能在超时内结束，已置禁止换文件位");
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

    /**
     * 批量记录（XADD_BULK/REMOVE_MULTI，C29/C30）是否可用：仅启用 AOF 且当前文件为 v2。
     * v1 存量文件写入 v2 专属操作码会导致旧语义回放器「未知操作」丢失整批，必须回退逐条。
     */
    public boolean isBulkRecordsEnabled() {
        return properties.isEnabled() && formatVersion >= AofFormat.FORMAT_VERSION_V2;
    }

    public void logTyped(String operation, String cacheKey, Object... args) {
        if (!properties.isEnabled()) return;
        byte opCode = AofFormat.opCode(operation);
        if (opCode == -1) {
            log.warn("未知AOF操作类型：{}", operation);
            return;
        }

        // §3.4 版本转换：调用方统一以相对 TTL 表达，落盘编码按文件版本分支
        int fv = formatVersion;
        Object[] aofArgs = args;
        if (opCode == AofFormat.OP_PUT_EXPIRED && fv >= AofFormat.FORMAT_VERSION_V2) {
            // C6：v2 以绝对过期时间戳落盘，消灭重启回放续期
            long ttl = args != null && args.length > 1 && args[1] instanceof Number
                    ? ((Number) args[1]).longValue() : 0L;
            long absExpire = ttl > 0 ? System.currentTimeMillis() + ttl : Long.MAX_VALUE;
            aofArgs = new Object[]{args != null && args.length > 0 ? args[0] : null, absExpire};
            opCode = AofFormat.OP_PUT_EXPIRED_ABS;
        } else if (opCode == AofFormat.OP_EXPIRE) {
            if (fv < AofFormat.FORMAT_VERSION_V2) {
                // v1 文件无法表达 EXPIRE（升级由重写完成）；跳过记日志，避免毒记录
                return;
            }
            // C7：容器 TTL，绝对时间戳落盘
            long ttl = args != null && args.length > 0 && args[0] instanceof Number
                    ? ((Number) args[0]).longValue() : 0L;
            long absExpire = ttl > 0 ? System.currentTimeMillis() + ttl : Long.MAX_VALUE;
            aofArgs = new Object[]{absExpire};
        }

        byte[] cmdBytes;
        try {
            cmdBytes = AofFormat.encodeCommand(fv, opCode, cacheKey != null ? cacheKey : "", aofArgs);
        } catch (IOException e) {
            // 编码失败（典型为 key 超长/非法控制符）：重试无意义，拒绝该条记录 + 告警 + 计数。
            // 不得走 handleWriteFailure（那是写盘失败的重试通道，语义不同）
            oversizedKeysRejected.incrementAndGet();
            log.error("AOF命令编码失败，该写操作不落盘：op={}, key={}, err={}",
                    operation, cacheKey, e.toString());
            return;
        } catch (RuntimeException e) {
            // C19：值不可编码（如非 Serializable 且非基础类型）抛 RuntimeException——
            // 业务写已生效，异常向调用方传播会被误判为操作失败而重试，LPUSH/SADD 等
            // 非幂等命令将产生重复数据。此处吞掉并告警计数（持久化缺口由对账兜底）
            encodeFailuresRejected.incrementAndGet();
            log.error("AOF命令值编码异常，该写操作不落盘：op={}, key={}, err={}",
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
        // C41：0x01/0x02 为迁移分片编码分隔符，含之的 key 写入成功但迁移解码必失败，前置拒绝
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c == 0x01 || c == 0x02) {
                throw new IllegalArgumentException(String.format(
                        "key 含非法控制符 0x0%X（位置 %d），写入被拒绝（C41）", (int) c, i));
            }
        }
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
            if (swapping) {
                // 旧流即将作废：只进换窗口专属缓冲，绝不写旧流（flush 语义由换文件后的新流承接）。
                // 与 rewriteBuffer 分离：换文件失败时可无重复地回放至旧流（见 swapPending 字段注释）。
                // 命令不丢；超限由重写线程承接中止。
                synchronized (swapPending) {
                    swapPending.add(cmdBytes);
                    rewriteBufferBytes.addAndGet(cmdBytes.length);
                }
                commandCount++;
                checkRewriteBufferLimit();
                return;
            }
            aofDataOut.write(cmdBytes);
            aofFileSize += cmdBytes.length;
            commandCount++;

            if (rewriting) {
                synchronized (rewriteBuffer) {
                    rewriteBuffer.add(cmdBytes);
                    rewriteBufferBytes.addAndGet(cmdBytes.length);
                }
                checkRewriteBufferLimit();
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

    /**
     * C12：双写缓冲字节上限检查。超限仅置中止标志（命令仍已在缓冲/旧流，不丢写），
     * 中止动作由重写线程在换文件前承接：复位状态 + 删 tmp，下个周期重来。
     */
    private void checkRewriteBufferLimit() {
        if (!rewriteAbort && rewriteBufferBytes.get() > properties.getRewriteBufferMaxBytes()) {
            rewriteAbort = true;
            rewriteAbortedOnOverflow.incrementAndGet();
            log.error("AOF重写双写缓冲超限（{} 字节 > 上限 {} 字节），本次重写将被中止，下个周期重来",
                    rewriteBufferBytes.get(), properties.getRewriteBufferMaxBytes());
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
            int fileVersion;
            if (magic == AofFormat.AOF_MAGIC) {
                fileVersion = AofFormat.FORMAT_VERSION_V1;
            } else if (magic == AofFormat.AOF_MAGIC_V2) {
                fileVersion = Math.max(AofFormat.FORMAT_VERSION_V2, reader.readVersion());
            } else {
                log.error("AOF文件魔数不匹配，期望 {:x} 或 {:x}，实际 {:x}",
                        AofFormat.AOF_MAGIC, AofFormat.AOF_MAGIC_V2, magic);
                return;
            }
            reader.setFormatVersion(fileVersion);
            // 写路径版本与文件实际版本对账（start 探测与回放头解析应一致）
            formatVersion = fileVersion;
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
            case AofFormat.OP_PUT_EXPIRED_ABS: {
                // C6：v2 绝对过期时间戳，重启回放不再续期
                Object v = args[0];
                long abs = args[1] instanceof Number ? ((Number) args[1]).longValue() : Long.MAX_VALUE;
                if (simple != null) {
                    simple.putWithExpireAt(key, v, abs);
                } else {
                    long ttl = abs == Long.MAX_VALUE ? 0L : abs - System.currentTimeMillis();
                    cache.put(key, v, ttl);
                }
                break;
            }
            case AofFormat.OP_EXPIRE: {
                // C7：容器 TTL 恢复（绝对时间戳；MAX_VALUE=清除 TTL）
                long abs = args[0] instanceof Number ? ((Number) args[0]).longValue() : Long.MAX_VALUE;
                if (simple != null) {
                    simple.expireAt(key, abs);
                }
                break;
            }
            case AofFormat.OP_XADD_BULK: {
                // C29：迁移导入批量 Stream 追加（回放幂等：xaddRaw putIfAbsent）
                if (simple != null && args.length >= 2
                        && args[0] instanceof List && args[1] instanceof List) {
                    List<?> flat = (List<?>) args[0];
                    List<?> fieldsList = (List<?>) args[1];
                    int n = Math.min(flat.size() / 2, fieldsList.size());
                    for (int i = 0; i < n; i++) {
                        long ms = ((Number) flat.get(i * 2)).longValue();
                        long seq = ((Number) flat.get(i * 2 + 1)).longValue();
                        @SuppressWarnings("unchecked")
                        Map<String, String> fields = (Map<String, String>) fieldsList.get(i);
                        simple.xaddRaw(key, new StreamId(ms, seq), fields);
                    }
                }
                break;
            }
            case AofFormat.OP_REMOVE_MULTI: {
                // C30：批量删除回放（幂等）
                if (args.length >= 1 && args[0] instanceof List) {
                    for (Object k : (List<?>) args[0]) {
                        cache.remove(String.valueOf(k));
                    }
                }
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
            case AofFormat.OP_HDEL: {
                if (simple != null && args.length >= 1 && args[0] instanceof List) {
                    List<?> fields = (List<?>) args[0];
                    String[] names = new String[fields.size()];
                    for (int i = 0; i < fields.size(); i++) {
                        names[i] = String.valueOf(fields.get(i));
                    }
                    simple.hdel(key, names);
                }
                break;
            }
            case AofFormat.OP_ZREM: {
                if (simple != null && args.length >= 1 && args[0] instanceof List) {
                    simple.zrem(key, ((List<?>) args[0]).toArray());
                }
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
                // 回放：恢复单条 PEL 条目（key, groupName, consumerName, idMs, idSeq [, deliveryCount]）
                String groupName = (String) args[0];
                String consumerName = (String) args[1];
                long idMs = ((Number) args[2]).longValue();
                long idSeq = ((Number) args[3]).longValue();
                int deliveryCount = args.length > 4 && args[4] instanceof Number
                        ? ((Number) args[4]).intValue() : 1;
                if (simple != null) {
                    simple.getStreamStore().restorePel(key, groupName, consumerName,
                            new StreamId(idMs, idSeq), deliveryCount);
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
                    // C38：v2 记录携带各条精确 deliveryCount（缺失=旧格式，退化为 claimTo）
                    List<Integer> counts = null;
                    if (args.length > 3 && args[3] instanceof List) {
                        counts = new ArrayList<Integer>();
                        for (Object o : (List<?>) args[3]) {
                            counts.add(o instanceof Number ? ((Number) o).intValue() : 1);
                        }
                    }
                    simple.xclaimRaw(key, groupName, consumerName, flat, counts);
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
            rewriteAbort = false;
            rewriteBufferBytes.set(0);
            synchronized (rewriteBuffer) {
                rewriteBuffer.clear();
            }
            synchronized (swapPending) {
                swapPending.clear();
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

            // 重写产物始终为当前最新版本（§3.4：首次重写后存量 v1 自然升级 v2）
            AofFormat.writeHeader(out, AofFormat.FORMAT_VERSION_LATEST);

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

            // 缓冲排空与 fsync 在 writeLock 之外执行（此时命令均双写旧流，清缓冲安全），
            // 写锁临界区只承担最终小量排空 + 换文件，保住 08-27 窄锁优化的性能意图（§3.2）
            synchronized (rewriteBuffer) {
                for (byte[] cmd : rewriteBuffer) {
                    out.write(cmd);
                }
                buffered += rewriteBuffer.size();
                rewriteBuffer.clear();
            }
            out.flush();
            tmpFos.getFD().sync();

            // —— C3/C12/C36 换文件状态机 ——
            // 前置中止检查：尚未进入换文件窗口，缓冲内命令旧流均有副本，
            // 中止 = 清缓冲 + 删 tmp，零丢写（C12 超限 / C36 停机保护）
            if (rewriteAbort || forbidSwap) {
                rewriteAbort = false;
                rewriteBufferBytes.set(0);
                synchronized (rewriteBuffer) {
                    rewriteBuffer.clear();
                }
                log.error("AOF重写中止：缓冲超限或停机禁止换文件，旧文件保持不变，下个周期重来");
                // 删除临时文件（Windows 需先关流再删；close 幂等，try-with-resources 重复关为空操作）
                out.close();
                if (tmpFile.exists()) {
                    tmpFile.delete();
                }
                return;
            }

            // ① 冻结旧流写入：此后写路径命令只进 swapPending，绝不写旧流；
            //    置位后立即进入同锁临界区，写者被 writeLock 阻塞，无命令落入作废窗口外的旧流。
            //    临界区排空量 = 冻结后至此刻的写入（有界），与现状同量级。
            synchronized (writeLock) {
                swapping = true;
            }
            List<byte[]> drained = null;
            int swapPendingCount = 0;
            try {
                synchronized (writeLock) {
                    // ② 最终排空 + 换文件一段完成（含冻结窗口内的写入），消灭丢写窗口（C3）
                    synchronized (rewriteBuffer) {
                        drained = new ArrayList<byte[]>(rewriteBuffer);
                        rewriteBuffer.clear();
                    }
                    synchronized (swapPending) {
                        swapPendingCount = swapPending.size();
                        drained.addAll(swapPending);
                        swapPending.clear();
                    }
                    rewriteBufferBytes.set(0);
                    for (byte[] cmd : drained) {
                        out.write(cmd);
                    }
                    buffered += drained.size();
                    out.flush();
                    tmpFos.getFD().sync();

                    // 换文件前关闭临时文件流：Windows 上 Files.move 要求源文件无打开句柄，
                    // 否则 move 抛 FileSystemException（POSIX 无此限制）。
                    // 关闭后 try-with-resources 的重复 close 为空操作（缓冲已排空、FOS close 幂等）。
                    out.close();

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
                        // 仅回放 swapPending 段（冻结窗口内命令旧流无副本；rewriteBuffer 段旧流已有，
                        // 重复回放会造成 LPUSH 类命令重复应用）；drained 尾部即该段（见上方填充顺序）
                        int replayFrom = drained.size() - swapPendingCount;
                        for (int i = replayFrom; i < drained.size(); i++) {
                            aofDataOut.write(drained.get(i));
                        }
                        aofDataOut.flush();
                        aofFileSize = aofFile.length();
                        throw moveFail;
                    }
                    aofRawOut = new FileOutputStream(aofFile, true);
                    aofOut = new BufferedOutputStream(aofRawOut, properties.getBufferSize());
                    aofDataOut = new DataOutputStream(aofOut);
                    aofFileSize = aofFile.length();
                    lastRewriteSize = aofFileSize;
                    // 新文件为 v2：写路径自此切换 v2 编码（PUT_EXPIRED_ABS/EXPIRE 生效）
                    formatVersion = AofFormat.FORMAT_VERSION_LATEST;
                }
            } finally {
                synchronized (writeLock) {
                    swapping = false;
                }
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
            // 空值标记（穿透防护）为运行期瞬态，不落盘不重写（回放缺失=缓存未命中，loader 重建）
            if (value == SimpleCache.nullMarker()) continue;

            byte[] cmd;
            if (entry.getExpireTime() == Long.MAX_VALUE) {
                cmd = AofFormat.encodeCommand(AofFormat.OP_PUT, key, value);
            } else {
                // v2：绝对过期时间戳（C6），重写产物不再携带相对 TTL
                cmd = AofFormat.encodeCommand(AofFormat.OP_PUT_EXPIRED_ABS, key, value,
                        entry.getExpireTime());
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
            count += writeExpireIfNeeded(out, key);
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
            count += writeExpireIfNeeded(out, key);
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
            count += writeExpireIfNeeded(out, key);
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
            count += writeExpireIfNeeded(out, key);
        }
        return count;
    }

    /**
     * C7：容器 TTL 持久化——有有限 TTL 的容器在成员记录后追加 OP_EXPIRE（绝对时间戳）。
     * 容器重建 API（hset/rpush/sadd/zadd）本身不携带 TTL，必须显式补一条才能跨重启保持。
     */
    private int writeExpireIfNeeded(DataOutputStream out, String key) throws IOException {
        long expireTime = cache.expireTimeAt(key);
        if (expireTime == Long.MAX_VALUE || expireTime < 0) {
            return 0;
        }
        byte[] cmd = AofFormat.encodeCommand(AofFormat.OP_EXPIRE, key, expireTime);
        out.write(cmd);
        return 1;
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
                    // 写该消费者持有的每条 PEL 条目（已投递未 XACK 的消息）；
                    // C38：携带精确 deliveryCount，回放后死信判断不受影响
                    for (StreamId pelId : ce.getValue().pendingIdsView()) {
                        PelEntry pel = group.getPel().get(pelId);
                        int deliveryCount = pel != null ? pel.getDeliveryCount() : 1;
                        byte[] pelCmd = AofFormat.encodeCommand(AofFormat.OP_XPEL_ENTRY, streamKey,
                                groupName, consumerName, pelId.getMsTime(), pelId.getSeq(), deliveryCount);
                        out.write(pelCmd);
                        count++;
                    }
                }
            }
        }
        return count;
    }

    /** 等待在途重写结束；超时仍未结束返回 false（C36 由 stop() 承接置位）。 */
    private boolean waitRewriteFinish() {
        if (rewriteThread != null && rewriteThread.isAlive()) {
            try {
                rewriteThread.join(30000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return rewriteThread == null || !rewriteThread.isAlive();
    }

    public AofStatus getStatus() {
        synchronized (writeLock) {
            return new AofStatus(lastFlushFailTime, consecutiveFailures,
                    rewriting ? rewriteBuffer.size() : 0, aofFileSize,
                    rewriting, commandCount, poisonRecordsSkipped.get(), crcFailures.get(),
                    oversizedKeysRejected.get(), rewriteBufferBytes.get(),
                    rewriteAbortedOnOverflow.get(), encodeFailuresRejected.get());
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
        private final long rewriteBufferBytes;
        private final long rewriteAbortedOnOverflow;
        private final long encodeFailuresRejected;

        public AofStatus(long lastFlushFailTime, int consecutiveFailures, int bufferSize,
                         long fileSizeBytes, boolean rewriting, long totalCommands,
                         long poisonRecordsSkipped, long crcFailures, long oversizedKeysRejected,
                         long rewriteBufferBytes, long rewriteAbortedOnOverflow,
                         long encodeFailuresRejected) {
            this.lastFlushFailTime = lastFlushFailTime;
            this.consecutiveFailures = consecutiveFailures;
            this.bufferSize = bufferSize;
            this.fileSizeBytes = fileSizeBytes;
            this.rewriting = rewriting;
            this.totalCommands = totalCommands;
            this.poisonRecordsSkipped = poisonRecordsSkipped;
            this.crcFailures = crcFailures;
            this.oversizedKeysRejected = oversizedKeysRejected;
            this.rewriteBufferBytes = rewriteBufferBytes;
            this.rewriteAbortedOnOverflow = rewriteAbortedOnOverflow;
            this.encodeFailuresRejected = encodeFailuresRejected;
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
        /** 重写双写缓冲当前累计字节（C12） */
        public long getRewriteBufferBytes() { return rewriteBufferBytes; }
        /** 重写因双写缓冲超限被中止的次数（C12） */
        public long getRewriteAbortedOnOverflow() { return rewriteAbortedOnOverflow; }
        /** 值编码异常被拒绝落盘的写操作次数（C19，不向业务传播） */
        public long getEncodeFailuresRejected() { return encodeFailuresRejected; }
    }
}
