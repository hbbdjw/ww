package com.ww.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地 Drive 管理器：加载配置中的磁盘路径，按 drivesPerSet 划分为多个 Set。
 *
 * <p>Set 划分规则：
 * <ul>
 *   <li>有效 drivesPerSet &gt; 0：每 drivesPerSet 个 Drive 组成一个 Set，最后一个 Set 可能较少</li>
 *   <li>drivesPerSet &lt;= 0（自动）：单机 = min(drives, 4)；集群 = min(节点数, drives, 4)</li>
 *   <li>确保每 Set 至少 1 个 Drive 且所有 Drive 被分配</li>
 * </ul>
 * Set 数量 = ceil(drives.size() / drivesPerSet)。
 */
public class DriveManager {

    private static final Logger log = LoggerFactory.getLogger(DriveManager.class);

    private final StorageProperties props;
    private final int nodeCount;

    private List<Drive> drives = Collections.emptyList();
    private List<StorageSet> sets = Collections.emptyList();
    private int drivesPerSet = 1;

    /**
     * @param props     storage 配置
     * @param nodeCount 集群节点数（单机传 1）。用于自动 drivesPerSet 推导。
     */
    public DriveManager(StorageProperties props, int nodeCount) {
        this.props = props;
        this.nodeCount = Math.max(1, nodeCount);
    }

    /** 初始化：加载磁盘、划分 Set。由 StorageConfig 在构造后调用 */
    public void init() {
        List<String> paths = props.getDrives();
        if (paths == null || paths.isEmpty()) {
            paths = List.of(props.getRootDir());
        }
        List<Drive> list = new ArrayList<>(paths.size());
        int idx = 0;
        for (String p : paths) {
            if (p == null || p.isBlank()) continue;
            Drive d = new Drive(idx++, p);
            d.ensureExists();
            list.add(d);
        }
        if (list.isEmpty()) {
            throw new StorageException("storage.drives 为空且 rootDir 无效，无法初始化任何 Drive");
        }
        this.drives = Collections.unmodifiableList(list);

        // 计算 drivesPerSet
        int dps = props.getDrivesPerSet();
        if (dps <= 0) {
            int auto;
            if (nodeCount <= 1) {
                // 单机：drivesPerSet = min(drives, 4)，所有盘归一个 Set
                auto = Math.min(drives.size(), 4);
            } else {
                // 集群：drivesPerSet = min(节点数, drives, 4)
                auto = Math.min(Math.min(nodeCount, drives.size()), 4);
            }
            dps = Math.max(1, auto);
        }
        if (dps > drives.size()) {
            log.warn("drivesPerSet={} 超过 Drive 数 {}，修正为 {}", dps, drives.size(), drives.size());
            dps = drives.size();
        }
        this.drivesPerSet = dps;

        // 划分 Set
        List<StorageSet> ss = new ArrayList<>();
        for (int start = 0; start < drives.size(); start += dps) {
            int end = Math.min(start + dps, drives.size());
            ss.add(new StorageSet(ss.size(), new ArrayList<>(drives.subList(start, end))));
        }
        this.sets = Collections.unmodifiableList(ss);

        log.info("DriveManager 初始化: drives={}, drivesPerSet={}, sets={}, nodeCount={}",
                drives.size(), drivesPerSet, sets.size(), nodeCount);
    }

    public List<Drive> getDrives() { return drives; }

    public List<StorageSet> getSets() { return sets; }

    public int getSetCount() { return sets.size(); }

    public int getDrivesPerSet() { return drivesPerSet; }

    public StorageSet getSet(int index) {
        if (index < 0 || index >= sets.size()) {
            throw new StorageException("Set index 越界: " + index + ", setCount=" + sets.size());
        }
        return sets.get(index);
    }

    /** 所有在线 Drive（供 list/scan 使用） */
    public List<Drive> onlineDrives() {
        List<Drive> r = new ArrayList<>();
        for (Drive d : drives) if (d.isOnline()) r.add(d);
        return r;
    }
}
