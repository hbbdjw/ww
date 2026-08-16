package com.ww.runtime.data.cache;

import com.ww.cache.AofProperties;
import com.ww.cache.AofService;
import com.ww.cache.CacheEntry;
import com.ww.cache.SimpleCache;
import com.ww.runtime.capability.CapabilityModule;
import com.ww.runtime.capability.PlacementView;
import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.ToIntFunction;

/**
 * CACHE 能力：以 {@link SimpleCache} 为本地引擎的分布式缓存节点能力。
 *
 * <p>集群调度（分区放置/期望态收敛/数据迁移）全部由 runtime 框架驱动：
 * 本模块只实现生命周期钩子（start/warm/drain/stop/freeze）、带 fencing 的
 * 本地读写与分区级数据搬迁 API（与 {@code MockCapability} 同构）。
 *
 * <h2>fencing 写入规则</h2>
 * {@link #put} / {@link #remove} 前校验 {@code view.owner(CACHE, partition)}：
 * 无属主 / 属主非本节点 / 纪元不符 → 抛 {@link StaleEpochException}。
 * 读不做 fencing（冻结/维护态仍可读）。
 *
 * <h2>迁移约束（首期）</h2>
 * 仅迁移 plain KV 条目（String/基础类型/Serializable 值）；Hash/List/Set/ZSet/
 * Stream 容器类型不参与迁移——{@link #exportPartition} 仅枚举 plain KV，
 * 容器类型 key 在 drain 冻结后随节点停止保留于本地。
 */
public final class CacheCapability implements CapabilityModule {

    private final NodeId self;
    private final PlacementView view;
    private final ToIntFunction<String> partitioner;
    private final boolean aofEnabled;
    private final File aofDir;

    private volatile SimpleCache<String, Object> cache;
    private volatile AofService aofService;
    private volatile boolean frozen;

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
        this.self = Objects.requireNonNull(self, "self");
        this.view = Objects.requireNonNull(view, "view");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        this.aofEnabled = aofEnabled;
        this.aofDir = aofDir;
        if (aofEnabled) {
            Objects.requireNonNull(aofDir, "aofDir required when aofEnabled");
        }
    }

    @Override
    public CapabilityType type() {
        return CapabilityType.CACHE;
    }

    @Override
    public void start() throws Exception {
        SimpleCache<String, Object> created = new SimpleCache<>();
        if (aofEnabled) {
            AofProperties props = new AofProperties();
            props.setEnabled(true);
            props.setDir(aofDir.getPath());
            AofService aof = new AofService(props, created);
            created.setAofService(aof);
            aof.start(); // AOF 回放（崩溃恢复）
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
            c.clear();
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

    /** 带 fencing 的删除。 */
    public boolean remove(String key) {
        requireWritable();
        Objects.requireNonNull(key, "key");
        checkOwner(partitionOf(key), -1L);
        boolean existed = cache.get(key) != null;
        cache.remove(key);
        return existed;
    }

    /** 读取（不做 fencing，冻结/维护态仍可读）。 */
    public Object get(String key) {
        Objects.requireNonNull(key, "key");
        SimpleCache<String, Object> c = cache;
        return c == null ? null : c.get(key);
    }

    /** 当前持有的 KV 总条数（含容器类型）。 */
    public long totalItems() {
        SimpleCache<String, Object> c = cache;
        return c == null ? 0L : c.size();
    }

    // ------------------------------------------------------------------
    // 迁移数据面 API（与 MockCapability 同构）
    // ------------------------------------------------------------------

    /**
     * 导出归属指定分区的全部 plain KV（字典序，迁移源端使用）。
     */
    public List<CachePartitionEntry> exportPartition(int partition) {
        SimpleCache<String, Object> c = requireStarted();
        List<CachePartitionEntry> result = new ArrayList<>();
        for (Map.Entry<String, CacheEntry<Object>> e : c.getAllKeyValueEntries()) {
            if (partitionOf(e.getKey()) != partition) {
                continue;
            }
            long remaining = c.remainingTtlMillis(e.getKey());
            // remainingTtlMillis: Long.MAX_VALUE 永不过期 → 归一为 <=0 语义
            long ttl = remaining == Long.MAX_VALUE ? 0L : remaining;
            result.add(new CachePartitionEntry(e.getKey(), ttl, e.getValue().getValue()));
        }
        result.sort(Comparator.comparing(CachePartitionEntry::key));
        return result;
    }

    /**
     * 导入条目到本地（迁移目标端使用，不做 fencing——导入时分区属主仍是源节点）。
     * 键不属于该分区直接拒绝。
     */
    public void importPartition(int partition, List<CachePartitionEntry> entries) {
        Objects.requireNonNull(entries, "entries");
        SimpleCache<String, Object> c = requireStarted();
        for (CachePartitionEntry e : entries) {
            if (partitionOf(e.key()) != partition) {
                throw new IllegalArgumentException(
                        "key '" + e.key() + "' does not belong to partition " + partition);
            }
            if (e.remainingTtlMillis() > 0) {
                c.put(e.key(), e.value(), e.remainingTtlMillis());
            } else {
                c.put(e.key(), e.value());
            }
        }
    }

    /** 清空指定分区的全部 plain KV（迁移源端在目标端确认后调用）。 */
    public void clearPartition(int partition) {
        SimpleCache<String, Object> c = requireStarted();
        for (String key : c.keys()) {
            if (partitionOf(key) == partition) {
                c.remove(key);
            }
        }
    }

    /** 指定分区当前持有的 plain KV 条数。 */
    public long partitionCount(int partition) {
        SimpleCache<String, Object> c = cache;
        if (c == null) {
            return 0L;
        }
        long count = 0L;
        for (Map.Entry<String, CacheEntry<Object>> e : c.getAllKeyValueEntries()) {
            if (partitionOf(e.getKey()) == partition) {
                count++;
            }
        }
        return count;
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /** 归属校验统一入口：无属主/异属主抛 StaleEpochException；expectedEpoch>=0 时还须纪元一致。 */
    private void checkOwner(int partition, long expectedEpoch) {
        Optional<PlacementView.Owner> owner = view.owner(type(), PartitionId.of(partition));
        if (owner.isEmpty()) {
            throw new StaleEpochException(expectedEpoch, -1L);
        }
        PlacementView.Owner o = owner.get();
        if (!o.node().equals(self) || (expectedEpoch >= 0 && o.epoch() != expectedEpoch)) {
            throw new StaleEpochException(expectedEpoch, o.epoch());
        }
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
