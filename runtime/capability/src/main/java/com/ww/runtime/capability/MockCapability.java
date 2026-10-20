package com.ww.runtime.capability;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.common.RuntimeFrameworkException;
import com.ww.runtime.common.StaleEpochException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToIntFunction;

/**
 * MOCK 能力的 fencing 参考实现：演示能力模块如何用 {@link PlacementView}
 * 做"旧写 fencing"，以及如何配合 MigrationEngine 完成分区级 KV 迁移
 * （DRAINING→MIGRATING 期间 export → 传输 → 对端 import → 源端 clear）。
 *
 * <p>写入校验规则（put 带 expectedEpoch）：
 * <ol>
 *   <li>按 partitioner 计算分区 p，查询 {@code view.owner(MOCK, p)}；</li>
 *   <li>无属主 / 属主非本节点 / 属主纪元与 expectedEpoch 不符 → 拒绝，
 *       抛 {@link StaleEpochException}(expectedEpoch, 实际纪元)；</li>
 *   <li>通过则写入本地 map 并计数。</li>
 * </ol>
 */
public final class MockCapability implements CapabilityModule {

    private final NodeId self;
    private final PlacementView view;
    private final ToIntFunction<String> partitioner;
    private final ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
    private final AtomicLong totalPuts = new AtomicLong();
    private volatile boolean frozen;

    /**
     * @param self        本节点标识（fencing 校验的"自己"）
     * @param view        放置视图端口
     * @param partitioner key → 分区号函数（必须返回非负整数）
     */
    public MockCapability(NodeId self, PlacementView view, ToIntFunction<String> partitioner) {
        this.self = Objects.requireNonNull(self, "self");
        this.view = Objects.requireNonNull(view, "view");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
    }

    @Override
    public CapabilityType type() {
        return CapabilityType.MOCK;
    }

    @Override
    public void start() {
        // 演示模块无重量级初始化：清空本地数据并复位冻结位即可安全重入
        data.clear();
        frozen = false;
    }

    @Override
    public void warm() {
        // 无需预热
    }

    @Override
    public void drain() {
        // 维护态：停新写（与 freeze 同效果，由状态机驱动）
        frozen = true;
    }

    @Override
    public void stop() {
        frozen = true;
        data.clear();
    }

    @Override
    public void freeze(boolean frozen) {
        this.frozen = frozen;
    }

    /**
     * 带 fencing 的写入：仅当分区属主为本节点且纪元与 expectedEpoch 一致时通过。
     *
     * @throws StaleEpochException       无属主 / 属主异节点 / 纪元不符
     * @throws RuntimeFrameworkException 模块处于冻结态（消息为 "frozen"）
     */
    public void put(String key, String value, long expectedEpoch) {
        requireWritable();
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        int partition = partitionOf(key);
        view.checkWritable(type(), PartitionId.of(partition), self, expectedEpoch);
        data.put(key, value);
        totalPuts.incrementAndGet();
    }

    /**
     * 便捷写入：自动使用 view 中该分区属主的当前纪元。
     *
     * @throws StaleEpochException 无属主 / 属主异节点
     */
    public void put(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        int partition = partitionOf(key);
        Optional<PlacementView.Owner> owner = view.owner(type(), PartitionId.of(partition));
        long actual = owner.map(PlacementView.Owner::epoch).orElse(-1L);
        if (owner.isEmpty() || !owner.get().node().equals(self)) {
            throw new StaleEpochException(-1L, actual);
        }
        put(key, value, actual);
    }

    /**
     * 读取（不做 fencing，冻结/维护态仍可读）。
     */
    public String get(String key) {
        Objects.requireNonNull(key, "key");
        return data.get(key);
    }

    /**
     * fencing 校验通过的累计写入次数（不含 importPartition 迁入）。
     */
    public int totalPuts() {
        return totalPuts.intValue();
    }

    /**
     * 当前持有的 KV 总条数。
     */
    public long totalItems() {
        return data.size();
    }

    /**
     * 各分区持有条数（key → 条数）。
     */
    public Map<Integer, Long> partitionCounts() {
        Map<Integer, Long> counts = new HashMap<>();
        for (String key : data.keySet()) {
            counts.merge(partitionOf(key), 1L, Long::sum);
        }
        return counts;
    }

    /**
     * 导出归属指定分区的全部 KV（迁移源端使用）。
     */
    public List<Map.Entry<String, String>> exportPartition(int partition) {
        List<Map.Entry<String, String>> result = new ArrayList<>();
        for (Map.Entry<String, String> e : data.entrySet()) {
            if (partitionOf(e.getKey()) == partition) {
                result.add(Map.entry(e.getKey(), e.getValue()));
            }
        }
        return result;
    }

    /**
     * 导入一个分区的 KV（迁移目标端使用，不计入 totalPuts）。
     */
    public void importPartition(int partition, List<Map.Entry<String, String>> entries) {
        Objects.requireNonNull(entries, "entries");
        for (Map.Entry<String, String> e : entries) {
            if (partitionOf(e.getKey()) != partition) {
                throw new IllegalArgumentException(
                        "key '" + e.getKey() + "' does not belong to partition " + partition);
            }
            data.put(e.getKey(), e.getValue());
        }
    }

    /**
     * 清空指定分区的全部 KV（迁移源端在目标端确认后调用）。
     */
    public void clearPartition(int partition) {
        for (String key : data.keySet()) {
            if (partitionOf(key) == partition) {
                data.remove(key);
            }
        }
    }

    /**
     * 指定分区当前持有条数。
     */
    public long partitionCount(int partition) {
        long count = 0;
        for (String key : data.keySet()) {
            if (partitionOf(key) == partition) {
                count++;
            }
        }
        return count;
    }

    /**
     * partitioner 归属判断的统一私有入口：所有分区相关 API 一律经由本方法，
     * 保证 put/export/import/clear/统计的归属口径一致。
     */
    private int partitionOf(String key) {
        int partition = partitioner.applyAsInt(key);
        if (partition < 0) {
            throw new IllegalArgumentException(
                    "partitioner returned negative partition for key '" + key + "': " + partition);
        }
        return partition;
    }

    private void requireWritable() {
        if (frozen) {
            throw new RuntimeFrameworkException("frozen");
        }
    }
}
