package com.ww.runtime.data.cache;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.IntPredicate;
import java.util.function.ToIntFunction;

import com.ww.runtime.capability.CapabilityModule;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;
import com.ww.runtime.data.cache.engine.AofProperties;
import com.ww.runtime.data.cache.engine.AofService;
import com.ww.runtime.data.cache.engine.CacheEntry;
import com.ww.runtime.data.cache.engine.Consumer;
import com.ww.runtime.data.cache.engine.ConsumerGroup;
import com.ww.runtime.data.cache.engine.SimpleCache;
import com.ww.runtime.data.cache.engine.StreamEntry;
import com.ww.runtime.data.cache.engine.StreamId;
import com.ww.runtime.data.cache.engine.StreamStore;

/**
 * CACHE 能力：以 {@link SimpleCache} 为本地引擎的分布式缓存节点能力。
 *
 * <p>集群调度（分区放置/期望态收敛/数据迁移）全部由 runtime 框架驱动：
 * 本模块只实现生命周期钩子（start/warm/drain/stop/freeze）、带 fencing 的
 * 本地读写与分区级数据搬迁 API（与 {@code MockCapability} 同构）。</p>
 *
 * <h2>fencing 写入规则</h2>
 * {@link #put} / {@link #remove} 前经 {@code view.checkWritable(CACHE, partition, self, epoch)}
 * 校验属主/纪元/新属主生效时间/本地租约：任一不满足 → 抛 {@link StaleEpochException}。
 * 生产装配使用本地缓存视图与租约，写路径不查询数据库。读不做 fencing（冻结/维护态仍可读）。</p>
 *
 * <h2>持久化（本地 AOF）</h2>
 * {@link #start()} 开启 AOF 并<b>回放</b>（崩溃恢复）；回放先于
 * {@code setAofService} 挂接——回放期间的写不会重复记日志（否则每次重启 AOF 翻倍）。
 * 回放恢复的旧归属数据由宿主启动清扫（{@link #retainPartitions}）按放置表裁剪。</p>
 *
 * <h2>迁移（全类型）</h2>
 * plain KV（String/JSON 基础类型）与 Hash/List/Set/ZSet/Stream（含消费组/PEL）
 * 均参与迁移：导出按值类型判别（与 AOF 重写的 getAll* 同口径），导入为
 * <b>整 key 替换</b>（幂等：重发/SYNCING 多轮重导不产生重复成员）；容器重建经
 * 公共写 API（自动记 AOF），Stream 经 Raw 重建 + 显式补记等价 AOF 命令，
 * 保证迁移落地的数据同时具备持久化语义。</p>
 */
public final class CacheCapability implements CapabilityModule {

    private final NodeId self;
    private final PlacementView view;
    private final ToIntFunction<String> partitioner;
    private final boolean aofEnabled;
    private final File aofDir;
    /** AOF 重写双写缓冲字节上限（<=0 用引擎默认，见 AofProperties） */
    private final long aofRewriteBufferMaxBytes;

    private volatile SimpleCache<String, Object> cache;
    private volatile AofService aofService;
    private volatile boolean frozen;
    /** 迁移 cutover 停写窗口：partition → 冻结到期（单调毫秒，TTL 自愈）。 */
    private final java.util.concurrent.ConcurrentHashMap<Integer, Long> frozenPartitions =
            new java.util.concurrent.ConcurrentHashMap<>();
    /** C16 第一步：缓存容量上限（key 数，&lt;=0 用引擎默认 100k） */
    private int cacheMaxSize = 0;
    /** 近似内存水位上限（字节，&lt;=0 不启用字节维度淘汰） */
    private long cacheMaxMemoryBytes = 0L;
    /** C16 第一步：单 stream 条目上限（&lt;=0 用引擎默认 100 万） */
    private long streamMaxLen = 0;
    /** C16 第一步：全 store PEL 总条目上限（&lt;=0 不限） */
    private long pelMaxEntries = 0;
    /** C16 第一步：stream 数量上限（&lt;=0 不限） */
    private long maxStreams = 0;

    /**
     * @param self        本节点标识（fencing 校验的“自己”）
     * @param view        放置视图端口
     * @param partitioner key → 分区号函数（必须与调度器同口径）
     * @param aofEnabled  是否启用 AOF 持久化（崩溃恢复）
     * @param aofDir      AOF 目录（aofEnabled=true 时必填）
     */
    public CacheCapability(NodeId self,
                           PlacementView view,
                           ToIntFunction<String> partitioner,
                           boolean aofEnabled,
                           File aofDir) {
        this(self, view, partitioner, aofEnabled, aofDir, 0L);
    }

    /**
     * @param aofRewriteBufferMaxBytes AOF 重写双写缓冲字节上限（C12，动态可配）；
     *                                 &lt;=0 用引擎默认 256MB。
     */
    public CacheCapability(NodeId self,
                           PlacementView view,
                           ToIntFunction<String> partitioner,
                           boolean aofEnabled,
                           File aofDir,
                           long aofRewriteBufferMaxBytes) {
        this.self = Objects.requireNonNull(self, "self");
        this.view = Objects.requireNonNull(view, "view");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        this.aofEnabled = aofEnabled;
        this.aofDir = aofDir;
        this.aofRewriteBufferMaxBytes = aofRewriteBufferMaxBytes;
        if (aofEnabled) {
            Objects.requireNonNull(aofDir, "aofDir required when aofEnabled");
        }
    }

    @Override
    public CapabilityType type() {
        return CapabilityType.CACHE;
    }

    /**
     * C16 第一步：内存容量上限配置化（fluent，start() 前调用）。
     * 超限语义 = 拒绝 + 告警（LRU 驱逐 / PEL 拒绝新投递 / stream 拒绝新建），
     * 各限值 {@code <=0} 走引擎默认或不限——默认不改变现网行为。
     *
     * @param cacheMaxSize  缓存 key 数上限（LRU 驱逐）
     * @param streamMaxLen  单 stream 条目上限
     * @param pelMaxEntries 全 store PEL 总条目上限
     * @param maxStreams    stream 数量上限
     */
    public CacheCapability withCapacityLimits(int cacheMaxSize, long streamMaxLen,
                                              long pelMaxEntries, long maxStreams) {
        return withCapacityLimits(cacheMaxSize, streamMaxLen, pelMaxEntries, maxStreams, 0L);
    }

    /** P0：新增近似字节水位（0=不启用），其余参数同四参重载。 */
    public CacheCapability withCapacityLimits(int cacheMaxSize, long streamMaxLen,
                                              long pelMaxEntries, long maxStreams,
                                              long cacheMaxMemoryBytes) {
        this.cacheMaxSize = cacheMaxSize;
        this.streamMaxLen = streamMaxLen;
        this.pelMaxEntries = pelMaxEntries;
        this.maxStreams = maxStreams;
        this.cacheMaxMemoryBytes = Math.max(0L, cacheMaxMemoryBytes);
        return this;
    }

    @Override
    public void start() throws Exception {
        SimpleCache<String, Object> created = new SimpleCache<>(null, cacheMaxSize,
                new StreamStore(streamMaxLen, pelMaxEntries, maxStreams), cacheMaxMemoryBytes);
        if (aofEnabled) {
            AofProperties props = new AofProperties();
            props.setEnabled(true);
            props.setDir(aofDir.getPath());
            if (aofRewriteBufferMaxBytes > 0) {
                props.setRewriteBufferMaxBytes(aofRewriteBufferMaxBytes);
            }
            AofService aof = new AofService(props, created);
            aof.start();          // 打开 AOF 文件（fsync/重写调度）
            aof.replay(created);  // 崩溃恢复回放：此时 cache 尚未挂接 aof，回放写不重复记日志
            created.setAofService(aof);
            this.aofService = aof;
        }
        this.cache = created;
        this.frozen = false;
    }

    @Override
    public void warm() {
        // 启动后台过期清理，降低孤儿条目堆积
        SimpleCache<String, Object> c = cache;
        if (c != null) {
            c.startEvictionScheduler(60_000L);
        }
    }

    @Override
    public void drain() {
        frozen = true; // 维护态：停新写（由状态机驱动）
    }

    @Override
    public void stop() {
        frozen = true;
        SimpleCache<String, Object> c = cache;
        if (c != null) {
            c.stopEvictionScheduler();
            if (aofService == null) {
                // 非持久模式：显式清内存。AOF 模式下 c.clear() 会把 CLEAR 落盘，
                // 优雅停机即抹掉全部持久化数据——实例整体丢弃交由 GC 即可。
                c.clear();
            }
        }
        AofService aof = aofService;
        if (aof != null) {
            try {
                aof.stop();
            } catch (RuntimeException e) {
                // 停机路径不抛：AOF 关闭失败仅记录由 SimpleCache 侧日志兜底
            }
        }
        this.cache = null;
        this.aofService = null;
    }

    @Override
    public void freeze(boolean frozen) {
        this.frozen = frozen;
    }

    // ------------------------------------------------------------------
    // 数据面 API（带 fencing）
    // ------------------------------------------------------------------

    /**
     * 带 fencing 的写入：仅当分区属主为本节点且纪元一致时通过。
     *
     * @param ttlMillis {@code >0} 相对过期毫秒；{@code <=0} 永不过期
     */
    public void put(String key, Object value, long ttlMillis) {
        requireWritable();
        Objects.requireNonNull(key, "key");
        checkOwner(partitionOf(key), -1L);
        if (ttlMillis > 0) {
            cache.put(key, value, ttlMillis);
        } else {
            cache.put(key, value);
        }
    }

    /**
     * 带 fencing 的“不存在才写”：key 已存在（未过期）时不修改值/TTL。
     *
     * @param ttlMillis {@code >0} 相对过期毫秒；{@code <=0} 永不过期
     * @return {@code true} 表示本次写入；{@code false} 表示 key 已存在
     */
    public boolean putIfAbsent(String key, Object value, long ttlMillis) {
        requireWritable();
        Objects.requireNonNull(key, "key");
        checkOwner(partitionOf(key), -1L);
        return ttlMillis > 0
                ? cache.putIfAbsent(key, value, ttlMillis)
                : cache.putIfAbsent(key, value);
    }

    /** 带 fencing 的删除。 */
    public boolean remove(String key) {
        requireWritable();
        Objects.requireNonNull(key, "key");
        checkOwner(partitionOf(key), -1L);
        boolean existed = cache.exists(key);
        cache.remove(key);
        return existed;
    }

    /** 读取（不做 fencing，冻结/维护态仍可读）。 */
    public Object get(String key) {
        Objects.requireNonNull(key, "key");
        SimpleCache<String, Object> c = cache;
        return c == null ? null : c.get(key);
    }

    /**
     * 剩余 TTL 毫秒（不做 fencing，数据面转发读用）。
     *
     * @return {@code -1} key 不存在；{@code Long.MAX_VALUE} 永不过期；其余为剩余毫秒
     */
    public long remainingTtlMillis(String key) {
        Objects.requireNonNull(key, "key");
        SimpleCache<String, Object> c = cache;
        return c == null ? -1L : c.remainingTtlMillis(key);
    }

    /** 当前持有的 key 总数（KV + 容器 + Stream）。 */
    public long totalItems() {
        SimpleCache<String, Object> c = cache;
        return c == null ? 0L : c.getIndexedKeyCount();
    }

    /**
     * 本地引擎是否已启动（网关/服务端路由容错判断用）：本节点能力被调度器停止后，
     * 数据已迁出——网关应跳过本地贡献继续聚合远端，而非把「未启动」当错误抛出。
     */
    public boolean isStarted() {
        return cache != null;
    }

    /** 本节点缓存统计（含内存水位、命中率、命令延迟）。 */
    public Map<String, Object> stats() {
        SimpleCache<String, Object> c = cache;
        if (c == null) {
            return Map.of("started", false);
        }
        Map<String, Object> out = new LinkedHashMap<>(c.stats());
        out.put("started", true);
        return out;
    }

    // ------------------------------------------------------------------
    // 统一命令通道（CacheCommandExecutor 同包访问）
    // ------------------------------------------------------------------

    SimpleCache<String, Object> commandEngine() {
        return requireStarted();
    }

    void checkCommandReadable(String key) {
        requireStarted();
        if (!servesPartitionOf(key)) {
            throw new StaleEpochException("not-owner: partition of '" + key + "' owned elsewhere");
        }
    }

    void checkCommandWritable(String key) {
        requireWritable();
        checkOwner(partitionOf(key), -1L);
    }

    int commandPartitionOf(String key) {
        return partitionOf(key);
    }

    /**
     * 枚举本地持有的全部 key（管理面浏览用，不做 fencing）。
     *
     * <p><b>管理面口径</b>：按 {@link SimpleCache#allKeys()} 枚举——包含任意值类型
     * （json 对象/数组值、容器、Stream、null 值）。值类型判别视图
     * （{@code getAllKeyValueEntries} 等按类型分类）仅供 AOF 重写与迁移导出使用，
     * 此处误用会造成「json 对象值/空值的 key 写入成功却在列表不可见」。</p>
     *
     * @param prefix 前缀过滤（null/空串表示不过滤）
     * @param limit  返回条数上限（&lt;=0 表示不限制；先全量匹配再排序截断，保证子集确定性）
     */
    public List<CacheKeyEntry> listKeys(String prefix, int limit) {
        SimpleCache<String, Object> c = requireStarted();
        boolean filter = prefix != null && !prefix.isEmpty();
        List<CacheKeyEntry> result = new ArrayList<>();
        for (String key : c.allKeys()) {
            if (filter && !key.startsWith(prefix)) {
                continue;
            }
            long ttl = visibleTtl(c, key);
            if (ttl == Long.MIN_VALUE) {
                continue;
            }
            result.add(new CacheKeyEntry(key, ttl));
        }
        result.sort(Comparator.comparing(CacheKeyEntry::key));
        if (limit > 0 && result.size() > limit) {
            result = new ArrayList<>(result.subList(0, limit));
        }
        return result;
    }

    /** 管理面 key 枚举条目：key + 剩余 TTL 毫秒（0/负数表示永不过期）。 */
    public record CacheKeyEntry(String key, long ttlMillis) {
    }

    /**
     * 统计本地持有的匹配前缀 key 数（管理面计数用，不做 fencing；与 {@link #listKeys}
     * 同口径——包含任意值类型）。
     *
     * @param prefix 前缀过滤（null/空串表示统计全部）
     */
    public long countKeys(String prefix) {
        SimpleCache<String, Object> c = requireStarted();
        boolean filter = prefix != null && !prefix.isEmpty();
        long count = 0L;
        for (String key : c.allKeys()) {
            if (filter && !key.startsWith(prefix)) {
                continue;
            }
            if (visibleTtl(c, key) != Long.MIN_VALUE) {
                count++;
            }
        }
        return count;
    }

    /**
     * 管理面可见 TTL：Stream 无 TTL 记为 0（永不过期）；KV 过期/缺失返回
     * {@link Long#MIN_VALUE} 表示不可见（避免与“永不过期=0”混淆）。
     */
    private static long visibleTtl(SimpleCache<String, Object> c, String key) {
        if (c.getStreamStore().hasStream(key)) {
            return 0L;
        }
        long remaining = c.remainingTtlMillis(key);
        if (remaining == -1L || remaining == 0L) {
            return Long.MIN_VALUE;
        }
        return remaining == Long.MAX_VALUE ? 0L : remaining;
    }

    // ------------------------------------------------------------------
    // 迁移数据面 API（全类型，与 MockCapability 同构）
    // ------------------------------------------------------------------

    /**
     * 导出归属指定分区的全部条目（字典序，迁移源端使用）。
     * 覆盖 plain KV + Hash/List/Set/ZSet + Stream（含消费组/PEL）。
     */
    public List<CachePartitionEntry> exportPartition(int partition) {
        SimpleCache<String, Object> c = requireStarted();
        List<CachePartitionEntry> result = new ArrayList<>();
        for (Map.Entry<String, CacheEntry<Object>> e : c.getAllKeyValueEntries()) {
            // 先判分区归属再构造载荷，避免为非本分区 key 做无谓拷贝
            if (partitionOf(e.getKey()) != partition) continue;
            addIfOwned(result, c, partition, e.getKey(), CachePartitionEntry.Kind.KV,
                    e.getValue().getValue());
        }
        for (Map.Entry<String, Map<String, Object>> e : c.getAllHashEntries()) {
            // SkipListMap 也是 Map——它属 ZSET（由 getAllZSetEntries 导出），此处跳过防双重计算
            if (e.getValue() instanceof java.util.concurrent.ConcurrentSkipListMap) {
                continue;
            }
            if (partitionOf(e.getKey()) != partition) continue;
            addIfOwned(result, c, partition, e.getKey(), CachePartitionEntry.Kind.HASH,
                    new LinkedHashMap<>(e.getValue()));
        }
        for (Map.Entry<String, java.util.concurrent.ConcurrentLinkedDeque<Object>> e
                : c.getAllListEntries()) {
            if (partitionOf(e.getKey()) != partition) continue;
            addIfOwned(result, c, partition, e.getKey(), CachePartitionEntry.Kind.LIST,
                    new java.util.ArrayList<>(e.getValue()));
        }
        for (Map.Entry<String, Set<Object>> e : c.getAllSetEntries()) {
            if (partitionOf(e.getKey()) != partition) continue;
            // 确定性排序（Set 迭代序不稳定，跨重启断点续跑依赖导出稳定性）
            List<Object> members = new java.util.ArrayList<>(e.getValue());
            members.sort(Comparator.comparing(String::valueOf));
            addIfOwned(result, c, partition, e.getKey(), CachePartitionEntry.Kind.SET, members);
        }
        for (Map.Entry<String, java.util.concurrent.ConcurrentSkipListMap<Double, Set<Object>>> e
                : c.getAllZSetEntries()) {
            if (partitionOf(e.getKey()) != partition) continue;
            List<CachePartitionEntry.ScoredMember> members = new ArrayList<>();
            for (Map.Entry<Double, Set<Object>> bucket : e.getValue().entrySet()) {
                for (Object member : bucket.getValue()) {
                    members.add(new CachePartitionEntry.ScoredMember(bucket.getKey(), member));
                }
            }
            addIfOwned(result, c, partition, e.getKey(), CachePartitionEntry.Kind.ZSET, members);
        }
        StreamStore streamStore = c.getStreamStore();
        for (String key : new java.util.TreeSet<>(streamStore.streamKeys())) {
            if (partitionOf(key) != partition) {
                continue;
            }
            result.add(new CachePartitionEntry(key, CachePartitionEntry.Kind.STREAM, 0L,
                    snapshotStream(streamStore, key)));
        }
        result.sort(ENTRY_ORDER);
        return result;
    }

    /**
     * 导入条目到本地（迁移目标端使用，不做 fencing——导入时分区属主仍是源节点）。
     * <b>整 key 替换</b>语义：容器先删后建（清除源端已删除的字段/成员），幂等。
     * 键不属于该分区直接拒绝。
     */
    public void importPartition(int partition, List<CachePartitionEntry> entries) {
        Objects.requireNonNull(entries, "entries");
        SimpleCache<String, Object> c = requireStarted();
        // (key, kind) 稳定排序：同名 key 的容器重建（remove）先于 STREAM 重建，
        // 避免容器 remove(key) 联动删除同名 stream 后又被 STREAM 条目恢复的正确顺序问题
        List<CachePartitionEntry> sorted = new ArrayList<>(entries);
        sorted.sort(ENTRY_ORDER);
        for (CachePartitionEntry e : sorted) {
            if (partitionOf(e.key()) != partition) {
                throw new IllegalArgumentException(
                        "key '" + e.key() + "' does not belong to partition " + partition);
            }
            switch (e.kind()) {
                case KV -> {
                    if (e.remainingTtlMillis() > 0) {
                        c.put(e.key(), e.payload(), e.remainingTtlMillis());
                    } else {
                        c.put(e.key(), e.payload());
                    }
                }
                case HASH -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> hash = (Map<String, Object>) e.payload();
                    c.remove(e.key());
                    for (Map.Entry<String, Object> field : hash.entrySet()) {
                        c.hset(e.key(), field.getKey(), field.getValue());
                    }
                    c.expire(e.key(), e.remainingTtlMillis());
                }
                case LIST -> {
                    @SuppressWarnings("unchecked")
                    List<Object> items = (List<Object>) e.payload();
                    c.remove(e.key());
                    if (!items.isEmpty()) {
                        c.rpush(e.key(), items.toArray());
                    }
                    c.expire(e.key(), e.remainingTtlMillis());
                }
                case SET -> {
                    @SuppressWarnings("unchecked")
                    List<Object> members = (List<Object>) e.payload();
                    c.remove(e.key());
                    if (!members.isEmpty()) {
                        c.sadd(e.key(), members.toArray());
                    }
                    c.expire(e.key(), e.remainingTtlMillis());
                }
                case ZSET -> {
                    @SuppressWarnings("unchecked")
                    List<CachePartitionEntry.ScoredMember> members =
                            (List<CachePartitionEntry.ScoredMember>) e.payload();
                    c.remove(e.key());
                    for (CachePartitionEntry.ScoredMember m : members) {
                        c.zadd(e.key(), m.score(), m.member());
                    }
                    c.expire(e.key(), e.remainingTtlMillis());
                }
                case STREAM -> importStream(c, e.key(),
                        (CachePartitionEntry.StreamSnapshot) e.payload());
            }
        }
    }

    /** 清空指定分区的全部条目（迁移源端在目标端确认后调用；覆盖全类型 + stream）。 */
    public void clearPartition(int partition) {
        SimpleCache<String, Object> c = requireStarted();
        // C30：收集后分批 removeMany（v2 文件聚合 REMOVE_MULTI 记录），
        // 大分区清空不再产生逐条 AOF 记录风暴
        java.util.Set<String> doomed = new java.util.LinkedHashSet<>();
        for (String key : c.allKeys()) {
            if (partitionOf(key) == partition) {
                doomed.add(key);
            }
        }
        removeInChunks(c, doomed);
    }

    /** 指定分区当前持有的条目数（全类型 + stream，与 exportPartition 同口径）。 */
    public long partitionCount(int partition) {
        SimpleCache<String, Object> c = cache;
        if (c == null) {
            return 0L;
        }
        long count = 0L;
        for (String key : c.allKeys()) {
            if (partitionOf(key) == partition) {
                count++;
            }
        }
        return count;
    }

    /**
     * 启动清扫：仅保留谓词 {@code keep} 通过的分区，删除其余分区数据（全类型）。
     * 供宿主在放置表引导后裁剪 AOF 回放出的旧归属数据。
     *
     * @return 删除的 key 数（0 表示无删除）
     */
    public long retainPartitions(IntPredicate keep) {
        Objects.requireNonNull(keep, "keep");
        SimpleCache<String, Object> c = cache;
        if (c == null) {
            return 0L;
        }
        // C30：收集后分批 removeMany（v2 聚合记录），启动清扫逐 key REMOVE 大军消除
        java.util.Set<String> doomed = new java.util.LinkedHashSet<>();
        for (String key : c.allKeys()) {
            if (!keep.test(partitionOf(key))) {
                doomed.add(key);
            }
        }
        return removeInChunks(c, doomed);
    }

    /**
     * 分批删除（C30）：每 {@link #AOF_BULK_CHUNK} 个 key 一次 {@code removeMany}——
     * 内存删除与 AOF 批记录同批提交，兼顾回放记录数与重写缓冲占用。
     *
     * @return 删除的 key 数
     */
    private long removeInChunks(SimpleCache<String, Object> c, java.util.Set<String> keys) {
        if (keys.isEmpty()) {
            return 0L;
        }
        long removed = 0L;
        List<String> chunk = new ArrayList<>(AOF_BULK_CHUNK);
        for (String key : keys) {
            chunk.add(key);
            if (chunk.size() >= AOF_BULK_CHUNK) {
                removed += c.removeMany(chunk);
                chunk = new ArrayList<>(AOF_BULK_CHUNK);
            }
        }
        if (!chunk.isEmpty()) {
            removed += c.removeMany(chunk);
        }
        return removed;
    }

    /** 持久化压实：触发 AOF 后台重写（未启用 AOF 时 no-op）。 */
    public void compactAof() {
        AofService aof = aofService;
        if (aof != null) {
            aof.bgRewriteAof();
        }
    }

    /**
     * 只读属主探测（数据面转发读路由纠偏用）：分区有属主且属主非本节点时返回 false。
     * 不做 fencing——冻结/维护态仍可读；无放置（单机退化）返回 true。
     */
    public boolean servesPartitionOf(String key) {
        Objects.requireNonNull(key, "key");
        if (!view.isReady(type())) {
            throw new RuntimeFrameworkException("placement view not ready for " + type());
        }
        Optional<PlacementView.Owner> owner = view.owner(type(), PartitionId.of(partitionOf(key)));
        return owner.isEmpty() || owner.get().node().equals(self);
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /** 导出排序：(key, kind)——同名 key 容器在前、STREAM 在后（见 importPartition 注释）。 */
    private static final Comparator<CachePartitionEntry> ENTRY_ORDER =
            Comparator.comparing(CachePartitionEntry::key)
                    .thenComparingInt(e -> e.kind().ordinal());

    /** 非过期条目才加入导出结果（TTL 归一：MAX_VALUE → 0 永不过期）。 */
    private void addIfOwned(List<CachePartitionEntry> result, SimpleCache<String, Object> c,
                            int partition, String key, CachePartitionEntry.Kind kind, Object payload) {
        if (partitionOf(key) != partition) {
            return;
        }
        long remaining = c.remainingTtlMillis(key);
        if (remaining == 0L) {
            return; // 已过期：不迁移（导入端将以永不过期语义复活）
        }
        long ttl = remaining == Long.MAX_VALUE ? 0L : remaining;
        result.add(new CachePartitionEntry(key, kind, ttl, payload));
    }

    /** Stream 快照：entries（ID 升序）+ 消费组（lastDeliveredId + 消费者 PEL）。 */
    private static CachePartitionEntry.StreamSnapshot snapshotStream(StreamStore store, String key) {
        List<Map.Entry<StreamId, StreamEntry>> sortedEntries = new ArrayList<>();
        for (Map.Entry<StreamId, StreamEntry> entry : store.getStreamEntries(key)) {
            sortedEntries.add(entry);
        }
        sortedEntries.sort(Map.Entry.comparingByKey());
        List<CachePartitionEntry.SnapshotEntry> entries = new ArrayList<>();
        for (Map.Entry<StreamId, StreamEntry> entry : sortedEntries) {
            StreamId id = entry.getKey();
            entries.add(new CachePartitionEntry.SnapshotEntry(
                    id.getMsTime(), id.getSeq(), new LinkedHashMap<>(entry.getValue().getFields())));
        }
        List<CachePartitionEntry.GroupSnapshot> groups = new ArrayList<>();
        Map<String, ConsumerGroup> groupMap = store.getStreamGroups(key);
        for (Map.Entry<String, ConsumerGroup> ge : new TreeMap<>(groupMap).entrySet()) {
            ConsumerGroup group = ge.getValue();
            StreamId lastDelivered = group.getLastDeliveredId();
            List<CachePartitionEntry.ConsumerSnapshot> consumers = new ArrayList<>();
            for (Map.Entry<String, Consumer> ce : new TreeMap<>(group.getConsumers()).entrySet()) {
                List<Long> pelIds = new ArrayList<>();
                for (StreamId pelId : ce.getValue().pendingIdsView()) {
                    pelIds.add(pelId.getMsTime());
                    pelIds.add(pelId.getSeq());
                }
                consumers.add(new CachePartitionEntry.ConsumerSnapshot(ce.getKey(), pelIds));
            }
            groups.add(new CachePartitionEntry.GroupSnapshot(ge.getKey(),
                    lastDelivered.getMsTime(), lastDelivered.getSeq(), consumers));
        }
        return new CachePartitionEntry.StreamSnapshot(entries, groups);
    }

    /**
     * Stream 补缺重建（幂等）：xaddRaw putIfAbsent / 组与消费者存在即跳过 / PEL 逐条恢复。
     * Raw 变体不落 AOF——导入后显式补记等价 AOF 命令，保证迁移落地的数据重启后可恢复。
     * <p>C29：v2 文件下 XADD 以 {@code XADD_BULK} 批量补记（≤{@value #AOF_BULK_CHUNK} 条/记录），
     * 百万级消息迁移的 AOF 增量从逐条降为批级；v1 存量文件回退逐条（回放兼容语义不变）。</p>
     */
    @SuppressWarnings("unchecked")
    private void importStream(SimpleCache<String, Object> c, String key,
                              CachePartitionEntry.StreamSnapshot snapshot) {
        StreamStore store = c.getStreamStore();
        AofService aof = aofService;
        boolean bulk = aof != null && aof.isBulkRecordsEnabled();
        List<Long> flat = new ArrayList<>();
        List<Map<String, String>> fieldBatch = new ArrayList<>();
        for (CachePartitionEntry.SnapshotEntry entry : snapshot.entries()) {
            Map<String, String> fields = entry.fields();
            c.xaddRaw(key, new StreamId(entry.idMs(), entry.idSeq()),
                    new LinkedHashMap<>(fields));
            if (bulk) {
                flat.add(entry.idMs());
                flat.add(entry.idSeq());
                fieldBatch.add(fields);
                if (fieldBatch.size() >= AOF_BULK_CHUNK) {
                    flushXaddBulk(key, flat, fieldBatch);
                    flat = new ArrayList<>();
                    fieldBatch = new ArrayList<>();
                }
            } else {
                logAof("XADD", key, entry.idMs(), entry.idSeq(), fields);
            }
        }
        if (bulk && !fieldBatch.isEmpty()) {
            flushXaddBulk(key, flat, fieldBatch);
        }
        for (CachePartitionEntry.GroupSnapshot group : snapshot.groups()) {
            String groupName = group.name();
            c.xgroupCreateRaw(key, groupName,
                    new StreamId(group.lastDeliveredMs(), group.lastDeliveredSeq()), true);
            logAof("XGROUP_CREATE", key, groupName,
                    group.lastDeliveredMs(), group.lastDeliveredSeq(), true);
            for (CachePartitionEntry.ConsumerSnapshot consumer : group.consumers()) {
                String consumerName = consumer.name();
                c.xgroupCreateConsumerRaw(key, groupName, consumerName);
                logAof("XGROUP_CREATECONSUMER", key, groupName, consumerName);
                for (int i = 0; i + 1 < consumer.pelIds().size(); i += 2) {
                    long ms = consumer.pelIds().get(i);
                    long seq = consumer.pelIds().get(i + 1);
                    StreamId id = new StreamId(ms, seq);
                    store.restorePel(key, groupName, consumerName, id);
                    logAof("XPEL_ENTRY", key, groupName, consumerName, ms, seq);
                }
            }
        }
    }

    /** 显式补记 AOF 命令（AOF 未启用/未挂接时 no-op；与 SimpleCache.logAof 同语义）。 */
    private void logAof(String op, String key, Object... args) {
        AofService aof = aofService;
        if (aof != null) {
            aof.logTyped(op, key, args);
        }
    }

    /** C29/C30：批量 AOF 记录的分块大小（单记录 ≤512 条，控制重写缓冲与回放内存）。 */
    private static final int AOF_BULK_CHUNK = 512;

    /** C29：提交一批 XADD 的聚合记录（args=[flatIds, fieldsList]，编码即时完成故可复用列表）。 */
    private void flushXaddBulk(String key, List<Long> flat, List<Map<String, String>> fieldsList) {
        logAof("XADD_BULK", key, flat, fieldsList);
    }

    /** 归属校验统一入口：本地视图负责 owner/epoch/validAfter/租约校验。 */
    private void checkOwner(int partition, long expectedEpoch) {
        checkPartitionFrozen(partition);
        if (!view.isReady(type())) {
            throw new StaleEpochException("placement view not ready for " + type());
        }
        view.checkWritable(type(), PartitionId.of(partition), self, expectedEpoch);
    }

    /**
     * 迁移 cutover 停写：冻结该分区写入 {@code ttlMillis}（单调计时，到期自动解除）。
     * 由迁移引擎在最终 SYNCING 前调用——停写窗口内不再有新写，导出的键集即最终集合。
     */
    public void freezePartition(int partition, long ttlMillis) {
        if (ttlMillis <= 0) {
            frozenPartitions.remove(partition); // ttl<=0 = 立即解冻（迁移失败/完成回收）
            return;
        }
        frozenPartitions.put(partition,
                System.nanoTime() / 1_000_000L + ttlMillis);
    }

    /** 分区冻结判定：TTL 到期即自动放行（执行节点崩溃不会永久锁死分区）。 */
    private void checkPartitionFrozen(int partition) {
        Long until = frozenPartitions.get(partition);
        if (until == null) {
            return;
        }
        if (System.nanoTime() / 1_000_000L < until) {
            throw new StaleEpochException("partition " + partition
                    + " frozen for migration cutover");
        }
        frozenPartitions.remove(partition, until);
    }

    private int partitionOf(String key) {
        int partition = partitioner.applyAsInt(key);
        if (partition < 0) {
            throw new IllegalArgumentException(
                    "partitioner returned negative partition for key '" + key + "': " + partition);
        }
        return partition;
    }

    private SimpleCache<String, Object> requireStarted() {
        SimpleCache<String, Object> c = cache;
        if (c == null) {
            throw new RuntimeFrameworkException("cache capability not started");
        }
        return c;
    }

    private void requireWritable() {
        if (frozen) {
            throw new RuntimeFrameworkException("frozen");
        }
        requireStarted();
    }
}
