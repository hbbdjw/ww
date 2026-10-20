package com.ww.runtime.data.cache.engine;

import java.io.Serializable;
import java.util.List;

/**
 * XAUTOCLAIM 返回结果：下一次扫描的 cursor + 实际认领的 PelEntry 列表。
 *
 * <p>{@code nextCursor == StreamId.MAX} 表示已扫描完整个 PEL。
 */
public final class XAutoClaimResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final StreamId nextCursor;
    private final List<PelEntry> claimed;

    public XAutoClaimResult(StreamId nextCursor, List<PelEntry> claimed) {
        this.nextCursor = nextCursor;
        this.claimed = claimed != null ? claimed : List.of();
    }

    public StreamId getNextCursor() {
        return nextCursor;
    }

    public List<PelEntry> getClaimed() {
        return claimed;
    }

    public boolean hasMore() {
        return nextCursor != null && nextCursor != StreamId.MAX;
    }

    @Override
    public String toString() {
        return "XAutoClaimResult{nextCursor=" + nextCursor
                + ", claimed=" + claimed.size() + '}';
    }
}
