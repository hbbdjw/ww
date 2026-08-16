package com.ww.storage;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 对象元数据 LRU 缓存：减少 HEAD/GET 命中已有对象时的磁盘 IO。
 *
 * <p>设计要点：
 * <ul>
 *   <li>仅缓存本地读取的元数据（主节点视角）——读路径必走主，写路径必经主，
 *       因此本地失效即可保证 read-after-write 一致性</li>
 *   <li>使用 {@link LinkedHashMap#removeEldestEntry} 实现 LRU 淘汰</li>
 *   <li>同步保护：缓存访问发生在虚拟线程或 Servlet 线程，需 thread-safe</li>
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

    private final LinkedHashMap<Key, ObjectMetadata> map;
    private final int maxSize;

    public ObjectMetadataCache(int maxSize) {
        if (maxSize <= 0) throw new IllegalArgumentException("maxSize 必须 > 0");
        this.maxSize = maxSize;
        this.map = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Key, ObjectMetadata> eldest) {
                return size() > maxSize;
            }
        };
    }

    public synchronized ObjectMetadata get(String bucket, String key) {
        if (bucket == null || key == null) return null;
        return map.get(new Key(bucket, key));
    }

    public synchronized void put(String bucket, String key, ObjectMetadata meta) {
        if (bucket == null || key == null || meta == null) return;
        map.put(new Key(bucket, key), meta);
    }

    public synchronized void invalidate(String bucket, String key) {
        if (bucket == null || key == null) return;
        map.remove(new Key(bucket, key));
    }

    /** 清空指定 Bucket 下所有缓存条目（best-effort） */
    public synchronized void invalidateBucket(String bucket) {
        if (bucket == null) return;
        map.entrySet().removeIf(e -> e.getKey().bucket().equals(bucket));
    }

    public synchronized void clear() {
        map.clear();
    }

    public synchronized int size() {
        return map.size();
    }

    public int getMaxSize() { return maxSize; }

    /** 缓存键：bucket + key 复合，record 自动生成 equals/hashCode */
    private record Key(String bucket, String key) {}
}
