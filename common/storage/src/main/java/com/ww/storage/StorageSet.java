package com.ww.storage;

import java.util.Collections;
import java.util.List;

/**
 * Set（集合）：一组 Drive 的逻辑组合（条带 Strip）。
 *
 * <p>分布式部署下，同一个 SetIndex 在每个节点上都有一份本地 Drive 子集，
 * 因此一个逻辑 Set 的 Drive 分布在不同节点上；单机模式下 Set 的 Drive 分布在不同磁盘。
 * 一个对象只存储在一个 Set 上（由 {@link SetLocator} 选定）。
 *
 * <p>当前实现：对象写入 Set 内某一个 Drive（按 key hash 选定），跨节点采用整对象复制（非纠删码）。
 * 预留纠删码扩展点：未来可将对象分片写入 Set 内全部 Drive。
 */
public class StorageSet {

    private final int index;
    private final List<Drive> drives;

    public StorageSet(int index, List<Drive> drives) {
        this.index = index;
        this.drives = Collections.unmodifiableList(drives);
    }

    public int getIndex() { return index; }

    public List<Drive> getDrives() { return drives; }

    public int driveCount() { return drives.size(); }

    /** 选择 Set 内某个 Drive（按 hash 选定，当前每对象单 Drive 落盘） */
    public Drive selectDrive(long hash) {
        if (drives.isEmpty()) {
            throw new StorageException("Set " + index + " 无可用 Drive");
        }
        int idx = (int) ((hash & Long.MAX_VALUE) % drives.size());
        return drives.get(idx);
    }
}
