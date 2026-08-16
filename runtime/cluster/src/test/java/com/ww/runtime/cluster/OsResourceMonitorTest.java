package com.ww.runtime.cluster;

import com.ww.runtime.common.ResourceSnapshot;
import com.ww.runtime.common.TestClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class OsResourceMonitorTest {

    @TempDir
    Path dataDir;

    @Test
    void snapshotIsValidAndStampedByClock() {
        TestClock clock = new TestClock(987_654_321L);
        OsResourceMonitor monitor = new OsResourceMonitor(dataDir.toFile(), clock);

        ResourceSnapshot snapshot = monitor.snapshot();

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.updatedAtMillis()).isEqualTo(987_654_321L);
        assertThat(snapshot.cpuPercent()).isBetween(0.0, 100.0);
        assertThat(snapshot.memTotalBytes()).isGreaterThanOrEqualTo(0L);
        assertThat(snapshot.memUsedBytes())
                .isGreaterThanOrEqualTo(0L)
                .isLessThanOrEqualTo(snapshot.memTotalBytes());
        assertThat(snapshot.diskTotalBytes()).isGreaterThanOrEqualTo(0L);
        assertThat(snapshot.diskUsedBytes())
                .isGreaterThanOrEqualTo(0L)
                .isLessThanOrEqualTo(snapshot.diskTotalBytes());
    }

    @Test
    void snapshotWithDefaultSystemClockIsNotNull() {
        OsResourceMonitor monitor = new OsResourceMonitor(dataDir.toFile());

        ResourceSnapshot snapshot = monitor.snapshot();

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.updatedAtMillis()).isPositive();
    }
}
