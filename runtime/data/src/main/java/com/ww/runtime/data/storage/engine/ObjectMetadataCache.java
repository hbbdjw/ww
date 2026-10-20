package com.ww.runtime.data.storage.engine;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 对象元数据缓存：减少 HEAD/GET 命中已有对象时的磁盘 IO。
 *
 * <p>设计要点：
 * <ul>
 *   <li>仅缓存本地主节点视角的元数据——读路径必走主，写路径必经主，
 *       因此本地失效即可保证 read-after-write 一致性</li>
 *   <li>基于 {@link ConcurrentHashMap}，无全局锁，不钉住虚拟线程</li>
 *   <li>近似 LRU 淘汰：容量超过 maxSize 时遍历批量移除
 *       （{@link ConcurrentHashMap} 迭代器对并发修改安全；
 *       元数据缓存场景可接受近似淘汰）</li>
 *   <li>容量上限可配置，默认 10,000 条（估算每条约 200B，总计约 2MB 堆内）</li>
 * </ul>
 *
 * <p>失效时机：
 * <ul>
 *   <li>PUT 覆盖写：用新元数据覆盖缓存（或失效）</li>
 *   <li>DELETE：从缓存移除</li>
 *   <li>Bucket 删除：清空该 bucket 下所有条目（best-effort，遍历清除）</li>
 * </ul>
 */
public class ObjectMetadataCache {

    private final ConcurrentHashMap<Key, ObjectMetadata> map;
    private final int maxSize;

    public ObjectMetadataCache(int maxSize) {
        if (maxSize <= 0) throw new IllegalArgumentException("maxSize 必须 > 0");
        this.maxSize = maxSize;
        this.map = new ConcurrentHashMap<>(16, 0.75f, 16);
    }

    public ObjectMetadata get(String bucket, String key) {
        if (bucket == null || key == null) return null;
        return map.get(new Key(bucket, key));
    }

    public void put(String bucket, String key, ObjectMetadata meta) {
        if (bucket == null || key == null || meta == null) return;
        map.put(new Key(bucket, key), meta);
        if (map.size() > maxSize) {
            evictExcess();
        }
    }

    public void invalidate(String bucket, String key) {
        if (bucket == null || key == null) return;
        map.remove(new Key(bucket, key));
    }

    /** 清空指定 Bucket 下所有缓存条目（best-effort） */
    public void invalidateBucket(String bucket) {
        if (bucket == null) return;
        map.entrySet().removeIf(e -> e.getKey().bucket().equals(bucket));
    }

    public void clear() {
        map.clear();
    }

    public int size() {
        return map.size();
    }

    public int getMaxSize() { return maxSize; }

    /** 超出容量时批量移除多余条目（近似 LRU：ConcurrentHashMap 无顺序，遍历移除即可） */
    private void evictExcess() {
        int toRemove = map.size() - maxSize;
        if (toRemove <= 0) return;
        Iterator<Map.Entry<Key, ObjectMetadata>> it = map.entrySet().iterator();
        while (it.hasNext() && toRemove > 0) {
            it.next();
            it.remove();
            toRemove--;
        }
    }

    /** 缓存键：bucket + key 复合，record 自动生成 equals/hashCode */
    private record Key(String bucket, String key) {}
}
