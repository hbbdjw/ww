package com.ww.runtime.data.storage.engine;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 对象元数据索引（T3 止血 + R6 抽出）：Drive → bucket → key 有序
 * （{@link ConcurrentSkipListMap}），行为与原 {@code ObjectStore.index} 字段严格等价，
 * 抽出为独立类承载容量上限与降级语义，接口不变、实现可换（专项 7D 持久化快照铺路）。
 *
 * <p><b>容量止血</b>：索引条目达到 {@code maxEntries} 后 {@link #put} 拒绝新增（返回 false），
 * 调用方将该桶标记为降级（{@link #degradeBucket}）——列举/计数改走目录扫描
 * （正确性 &gt; O(logN)）。删除照常递减计数（空间可回收，但已降级桶不自动恢复，
 * 避免列举路径在索引/扫描间来回切换产生语义抖动）。
 *
 * <p>按 Drive 隔离保持列举既有语义：离线 Drive 上的对象不参与列举/计数。
 */
public final class MetadataIndex {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataIndex.class);

    private final long maxEntries;
    private final Map<Drive, ConcurrentHashMap<String, ConcurrentSkipListMap<String, ObjectMetadata>>> index
            = new ConcurrentHashMap<>();
    private final AtomicLong totalEntries = new AtomicLong();
    /** 索引容量满后被拒绝新增的桶：列举/计数降级目录扫描。 */
    private final Set<String> degradedBuckets = ConcurrentHashMap.newKeySet();
    /** 因索引容量满被拒绝的索引登记次数（可观测性红线：不得静默）。 */
    private final AtomicLong rejectedOnCapacity = new AtomicLong();
    /** T22：索引变更代数（写/删/清空/删桶时自增）——分区计数快照复用的失效依据。 */
    private final AtomicLong generation = new AtomicLong();

    /** @param maxEntries 容量上限（条目数）；&lt;=0 表示不限。 */
    public MetadataIndex(long maxEntries) {
        this.maxEntries = maxEntries;
    }

    /** 取（不存在则建）指定 Drive 上桶的有序索引。仅写路径使用——允许创建空映射。 */
    public ConcurrentSkipListMap<String, ObjectMetadata> indexOf(Drive drive, String bucket) {
        return index.computeIfAbsent(drive, d -> new ConcurrentHashMap<>())
                .computeIfAbsent(bucket, b -> new ConcurrentSkipListMap<>());
    }

    /** 只读取指定 Drive 上桶的有序索引（不存在返回 null，不创建空映射）。 */
    public ConcurrentSkipListMap<String, ObjectMetadata> peekIndex(Drive drive, String bucket) {
        Map<String, ConcurrentSkipListMap<String, ObjectMetadata>> byBucket = index.get(drive);
        return byBucket == null ? null : byBucket.get(bucket);
    }

    /** 指定 Drive 上已有索引的桶名快照（统计遍历用）。 */
    public Set<String> bucketsOn(Drive drive) {
        Map<String, ConcurrentSkipListMap<String, ObjectMetadata>> byBucket = index.get(drive);
        return byBucket == null ? Set.of() : Set.copyOf(byBucket.keySet());
    }

    /** 全部 Drive 上已有索引的桶名并集快照（T9 启动期桶注册对账用）。 */
    public Set<String> bucketsSnapshot() {
        Set<String> all = new java.util.HashSet<>();
        for (Map<String, ConcurrentSkipListMap<String, ObjectMetadata>> byBucket : index.values()) {
            all.addAll(byBucket.keySet());
        }
        return all;
    }

    /**
     * 索引登记（容量感知）：容量未满返回 true；容量满返回 false（不插入），
     * 首次触发时告警。调用方拿到 false 须 {@link #degradeBucket} 保证列举正确性。
     * <p>覆盖既有键为就地更新不增容量，不受上限拦截；调用方持同键条带锁，
     * check-then-act 安全。重建路径用 {@link #putDirect}（不受容量限制）。</p>
     * <p>T22：无论登记是否被容量拒绝都推进代数——拒绝发生在<b>数据已落盘之后</b>，
     * 磁盘可见集合已变，快照复用缓存必须失效。</p>
     */
    public boolean putGuarded(Drive drive, String bucket, String key, ObjectMetadata meta) {
        ConcurrentSkipListMap<String, ObjectMetadata> idx = indexOf(drive, bucket);
        if (idx.containsKey(key)) {
            idx.put(key, meta); // 就地更新：不新增条目，不受容量拦截
            generation.incrementAndGet();
            return true;
        }
        if (maxEntries > 0 && totalEntries.get() >= maxEntries) {
            rejectLog(bucket);
            generation.incrementAndGet();
            return false;
        }
        idx.put(key, meta);
        totalEntries.incrementAndGet();
        generation.incrementAndGet();
        return true;
    }

    /**
     * 容量感知的版本合并登记（副本路径：仅保留不低于既有的版本）。
     * 语义同 {@link #putGuarded}：新增条目受容量拦截，命中既有键不拦截。
     */
    /**
     * 容量感知的版本合并登记（副本路径：仅保留不低于既有的版本）。
     * 语义同 {@link #putGuarded}：新增条目受容量拦截，命中既有键不拦截；
     * 拒绝分支同样推进代数（数据已落盘，磁盘可见集已变）。
     */
    public boolean mergeGuarded(Drive drive, String bucket, String key, ObjectMetadata meta) {
        ConcurrentSkipListMap<String, ObjectMetadata> idx = indexOf(drive, bucket);
        boolean isNew = !idx.containsKey(key);
        if (isNew && maxEntries > 0 && totalEntries.get() >= maxEntries) {
            rejectLog(bucket);
            generation.incrementAndGet();
            return false;
        }
        idx.merge(key, meta, (a, b) -> a.getVersion() >= b.getVersion() ? a : b);
        if (isNew) {
            totalEntries.incrementAndGet();
        }
        generation.incrementAndGet();
        return true;
    }

    private void rejectLog(String bucket) {
        if (rejectedOnCapacity.incrementAndGet() == 1) {
            LOG.error("元数据索引达到容量上限 {}，停止索引新增；桶 {} 起列举降级目录扫描"
                    + "（正确性优先，索引快照持久化为后续专项）", maxEntries, bucket);
        }
    }

    /** 直接装入（重建路径）：不受容量限制，计入总数。 */
    public void putDirect(Drive drive, String bucket, String key, ObjectMetadata meta) {
        indexOf(drive, bucket).put(key, meta);
        totalEntries.incrementAndGet();
        generation.incrementAndGet();
    }

    /**
     * 索引摘除（存在才递减计数）。
     * <p>T22：无条件推进代数——降级桶的删除在索引中本就无条目（磁盘已变），
     * 不能依赖"条目命中才失效"，否则快照缓存残留已删对象。</p>
     */
    public void remove(Drive drive, String bucket, String key) {
        ConcurrentSkipListMap<String, ObjectMetadata> idx = peekIndex(drive, bucket);
        if (idx != null && idx.remove(key) != null) {
            totalEntries.decrementAndGet();
        }
        generation.incrementAndGet();
    }

    /** 重建前清空（含降级状态与计数）。 */
    public void clear() {
        index.clear();
        totalEntries.set(0);
        degradedBuckets.clear();
        rejectedOnCapacity.set(0);
        generation.incrementAndGet();
    }

    /** 清除指定桶的索引（删桶成功后调用），同步回收计数。 */
    public void dropBucket(String bucket) {
        boolean changed = false;
        for (Map<String, ConcurrentSkipListMap<String, ObjectMetadata>> byBucket : index.values()) {
            NavigableMap<String, ObjectMetadata> removed = byBucket.remove(bucket);
            if (removed != null) {
                totalEntries.addAndGet(-removed.size());
                changed = true;
            }
        }
        if (degradedBuckets.remove(bucket)) {
            changed = true;
        }
        if (changed) {
            generation.incrementAndGet();
        }
    }

    /** 标记桶降级：列举/计数改走目录扫描（索引容量满后的正确性兜底）。 */
    public void degradeBucket(String bucket) {
        if (degradedBuckets.add(bucket)) {
            generation.incrementAndGet(); // 计数口径变化：分区计数快照需失效
            LOG.warn("桶 {} 元数据列举降级为目录扫描（索引容量上限 {}）", bucket, maxEntries);
        }
    }

    public boolean isDegraded(String bucket) {
        return degradedBuckets.contains(bucket);
    }

    public long totalEntries() {
        return totalEntries.get();
    }

    /** T22：当前索引代数（任何登记/摘除/清空/删桶后变化）——分区计数快照复用依据。 */
    public long generation() {
        return generation.get();
    }

    public long getRejectedOnCapacity() {
        return rejectedOnCapacity.get();
    }

    public Set<String> degradedBucketsView() {
        return java.util.Collections.unmodifiableSet(degradedBuckets);
    }
}
