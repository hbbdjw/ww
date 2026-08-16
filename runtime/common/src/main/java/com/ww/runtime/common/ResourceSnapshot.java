package com.ww.runtime.common;

/**
 * 某一时刻的节点资源快照。
 */
public record ResourceSnapshot(
        double cpuPercent,
        long memTotalBytes,
        long memUsedBytes,
        long diskTotalBytes,
        long diskUsedBytes,
        long updatedAtMillis) {

    public ResourceSnapshot {
        if (!Double.isFinite(cpuPercent) || cpuPercent < 0 || cpuPercent > 100) {
            throw new IllegalArgumentException("cpuPercent must be in [0, 100], got " + cpuPercent);
        }
        if (memTotalBytes < 0 || memUsedBytes < 0 || memUsedBytes > memTotalBytes) {
            throw new IllegalArgumentException(
                    "invalid memory usage: used=" + memUsedBytes + ", total=" + memTotalBytes);
        }
        if (diskTotalBytes < 0 || diskUsedBytes < 0 || diskUsedBytes > diskTotalBytes) {
            throw new IllegalArgumentException(
                    "invalid disk usage: used=" + diskUsedBytes + ", total=" + diskTotalBytes);
        }
    }

    public static ResourceSnapshot of(double cpuPercent,
                                      long memTotalBytes,
                                      long memUsedBytes,
                                      long diskTotalBytes,
                                      long diskUsedBytes,
                                      long updatedAtMillis) {
        return new ResourceSnapshot(cpuPercent, memTotalBytes, memUsedBytes,
                diskTotalBytes, diskUsedBytes, updatedAtMillis);
    }

    public double memUsedRatio() {
        return memTotalBytes == 0 ? 0.0 : (double) memUsedBytes / memTotalBytes;
    }

    public double diskUsedRatio() {
        return diskTotalBytes == 0 ? 0.0 : (double) diskUsedBytes / diskTotalBytes;
    }
}
