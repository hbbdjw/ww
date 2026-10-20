package com.ww.runtime.data.cache.engine;

import java.io.Serializable;

/**
 * XINFO STREAM 返回的 stream 元信息快照。
 *
 * <p>对应 Redis XINFO STREAM 输出子集：length / first-entry / last-entry。
 */
public final class StreamInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long length;
    private final StreamId firstId;
    private final StreamId lastId;
    private final StreamEntry firstEntry;
    private final StreamEntry lastEntry;
    /** 消费者组数量。 */
    private final int groupsCount;

    public StreamInfo(long length,
                      StreamId firstId,
                      StreamId lastId,
                      StreamEntry firstEntry,
                      StreamEntry lastEntry,
                      int groupsCount) {
        this.length = length;
        this.firstId = firstId;
        this.lastId = lastId;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.groupsCount = groupsCount;
    }

    public long getLength() {
        return length;
    }

    public StreamId getFirstId() {
        return firstId;
    }

    public StreamId getLastId() {
        return lastId;
    }

    public StreamEntry getFirstEntry() {
        return firstEntry;
    }

    public StreamEntry getLastEntry() {
        return lastEntry;
    }

    public int getGroupsCount() {
        return groupsCount;
    }

    @Override
    public String toString() {
        return "StreamInfo{length=" + length
                + ", firstId=" + firstId
                + ", lastId=" + lastId
                + ", groups=" + groupsCount + '}';
    }
}
