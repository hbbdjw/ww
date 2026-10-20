package com.ww.runtime.data.storage.engine;

import java.util.Collections;
import java.util.List;

/**
 * Set（集合）：一组 Drive 的逻辑组合（条带 Strip）。
 *
 * <p>Set 只承担节点内磁盘分组语义：一个对象只存储在一个 Set 上（由 {@link SetLocator} 选定），
 * 写入 Set 内某一个 Drive（按 key hash 选定）。
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
