package com.ww.runtime.cluster;

import com.ww.runtime.common.Clock;
import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.SystemClock;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Objects;

/**
 * 基于 OS 指标的 {@link ResourceMonitor} 实现：
 *
 * <ul>
 *   <li>CPU：{@link com.sun.management.OperatingSystemMXBean#getCpuLoad()}（系统级，[0,1]，不可用时为 -1）；</li>
 *   <li>内存：{@code getTotalMemorySize()} / {@code getFreeMemorySize()}；</li>
 *   <li>磁盘：对 dataDir 用 {@link File#getTotalSpace()} / {@link File#getUsableSpace()}。</li>
 * </ul>
 *
 * <p>采样时刻经 {@link Clock} 获取（默认 {@link SystemClock}），便于测试注入。</p>
 */
public final class OsResourceMonitor implements ResourceMonitor {

    private final com.sun.management.OperatingSystemMXBean osBean;
    private final File dataDir;
    private final Clock clock;

    public OsResourceMonitor(File dataDir) {
        this(dataDir, SystemClock.instance());
    }

    public OsResourceMonitor(File dataDir, Clock clock) {
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.osBean = ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class);
    }

    @Override
    public ResourceSnapshot snapshot() {
        double cpuPercent = clampPercent(osBean.getCpuLoad() * 100.0);

        long memTotal = Math.max(0L, osBean.getTotalMemorySize());
        long memFree = Math.max(0L, osBean.getFreeMemorySize());
        long memUsed = Math.max(0L, memTotal - memFree);

        long diskTotal = Math.max(0L, dataDir.getTotalSpace());
        long diskUsable = Math.max(0L, dataDir.getUsableSpace());
        long diskUsed = Math.max(0L, diskTotal - diskUsable);

        return ResourceSnapshot.of(cpuPercent, memTotal, memUsed, diskTotal, diskUsed, clock.nowMillis());
    }

    /** getCpuLoad 不可用时返回 -1，统一收敛到 [0,100]。 */
    private static double clampPercent(double value) {
        if (!Double.isFinite(value) || value < 0.0) {
            return 0.0;
        }
        return Math.min(100.0, value);
    }
}
