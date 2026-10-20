package com.ww.runtime.data.storage.engine;

import java.util.ArrayList;
import java.util.List;

/**
 * 对象存储本地配置（纯 POJO，由宿主装配层构造注入，映射 runtime.capabilities.storage-*）。
 *
 * <p>承载 storage 自身的本地磁盘、Set 划分、加密开关、IO 缓冲等配置；
 * 集群归属路由由 runtime placement 决定，此处不含任何集群拓扑配置。
 */
public class StorageProperties {

    /** 本地 Drive（磁盘）根路径列表。所有对象数据落盘于此。 */
    private List<String> drives = new ArrayList<>();

    /** 当 drives 为空时使用的默认根目录。 */
    private String rootDir = "./data/storage";

    /**
     * 每个 Set 包含的 Drive 数量。<=0 表示自动：
     * <ul>
     *   <li>集群模式：= min(集群节点数, 本地 Drive 数, 4)，确保同一 Set 的 Drive 分布在不同节点</li>
     *   <li>单机模式：= min(本地 Drive 数, 4)</li>
     * </ul>
     * Set 数量 = 本地 Drive 数 / drivesPerSet（必须整除，否则启动失败）。
     */
    private int drivesPerSet = 0;

    /** 是否启用加密（预留）。默认 false，启用后由 CryptoCodec 实现加解密。 */
    private boolean cryptoEnabled = false;

    /**
     * IO 直接内存缓冲区大小（字节）。用于 FileChannel 读写与 BufferedInputStream 缓冲。
     * 默认 64KB，建议 64KB~256KB。分配在堆外，不增加 GC 压力。
     */
    private int ioBufferSize = 64 * 1024;

    /**
     * 直接内存缓冲池总量上限（缓冲区个数）。每个缓冲区大小 = {@link #ioBufferSize}。
     * T16 起该值同时约束「在借 + 空闲」总量（默认 256 个 × 64KB = 16MB 堆外硬上限）：
     * 超额借出限时等待 5s 后拒绝（背压传导至调用方），消灭旧实现无界新分配的堆外 OOM。
     */
    private int bufferPoolMaxSize = 256;

    /**
     * 对象元数据 LRU 缓存上限（条目数）。仅缓存本地主节点视角的元数据。
     * 默认 10,000 条，估算每条约 200B，总计约 2MB 堆内。0 表示禁用缓存。
     */
    private int metadataCacheMaxSize = 10_000;

    /**
     * 单对象大小上限（字节）。REST 上传按 Content-Length 前置校验；
     * 迁移导出按此上限拒绝超大对象。默认 1GB。
     */
    private long maxObjectSize = 1024L * 1024 * 1024;

    /**
     * 元数据索引容量上限（条目数，T3 止血）。超限后停止索引新增并告警，
     * 受影响桶的列举/计数降级为目录扫描（正确性优先于 O(logN)）。
     * 0 表示不限。默认 2000 万条。
     */
    private long metadataIndexMaxEntries = 20_000_000L;

    /**
     * 迁移导入暂存聚合的并发数上限（T1）。超限拒绝新建聚合 + 告警，
     * 防止大量在途对象同时暂存挤占磁盘/句柄。默认 1024。
     */
    private int importStagingMaxAggregates = 1024;

    /**
     * 迁移导入暂存聚合的超时分钟数（T1）。流中断无 fin 片的暂存文件
     * 超期由守护清扫删除，防止泄漏。默认 30 分钟。
     */
    private int importStagingTimeoutMinutes = 30;

    /**
     * T9 灰度开关：PUT 隐式建桶检查。默认 false = 告警模式（隐式建桶照旧，仅 WARN+计数）；
     * 观察期后切 true = 严格模式（未建桶的 PUT 被拒绝）。
     */
    private boolean strictBucketCheck = false;

    /**
     * T13：副本写路径是否跳过 fsync。默认 false（与主路径一致做 fsync），
     * 磁盘密集且可容忍副本弱持久化的场景可置 true。
     */
    private boolean replicaSkipFsync = false;

    /**
     * T11：崩溃残留临时文件清扫的年龄阈值（毫秒）。启动清扫 + 周期 GC 共用，
     * 仅删除超过阈值的 object.*.tmp 残留。默认 1 小时。
     */
    private long tempSweepAgeMillis = 3_600_000L;

    /**
     * T11：崩溃残留周期 GC 间隔（毫秒）。0 表示关闭周期 GC（仅启动清扫）。默认 1 小时。
     */
    private long tempGcIntervalMillis = 3_600_000L;

    /**
     * P0-1 半同步复制总开关。默认 false（保持「主写落盘即应答、副本异步补齐」的既有语义，
     * 滚动升级安全）；true 时主写落盘后须在 {@link #semiSyncTimeoutMs} 内获得至少一个
     * 存活副本确认才应答客户端——杜绝「已 ack 的写随主节点死亡而静默丢失」。
     */
    private boolean semiSyncEnabled = false;

    /**
     * P0-1 半同步等待副本确认的超时（毫秒）。默认 2000（内网 gRPC 复制正常毫秒级；
     * 副本不可达时写延迟至多增加该值，由 {@link #semiSyncFailClosed} 决定失败还是降级）。
     */
    private long semiSyncTimeoutMs = 2_000L;

    /**
     * P0-1 半同步超时策略。默认 false = 降级应答（WARN + at-risk 观测后照常返回，
     * 可用性优先）；true = fail-closed（抛 {@code ReplicationTimeoutException} → 503，
     * 一致性优先：宁可写失败也不应答可能丢失的请求）。
     */
    private boolean semiSyncFailClosed = false;

    public List<String> getDrives() { return drives; }
    public void setDrives(List<String> drives) { this.drives = drives; }

    public String getRootDir() { return rootDir; }
    public void setRootDir(String rootDir) { this.rootDir = rootDir; }

    public int getDrivesPerSet() { return drivesPerSet; }
    public void setDrivesPerSet(int drivesPerSet) { this.drivesPerSet = drivesPerSet; }

    public boolean isCryptoEnabled() { return cryptoEnabled; }
    public void setCryptoEnabled(boolean cryptoEnabled) { this.cryptoEnabled = cryptoEnabled; }

    public int getIoBufferSize() { return ioBufferSize; }
    public void setIoBufferSize(int ioBufferSize) { this.ioBufferSize = ioBufferSize; }

    public int getBufferPoolMaxSize() { return bufferPoolMaxSize; }
    public void setBufferPoolMaxSize(int bufferPoolMaxSize) { this.bufferPoolMaxSize = bufferPoolMaxSize; }

    public int getMetadataCacheMaxSize() { return metadataCacheMaxSize; }
    public void setMetadataCacheMaxSize(int metadataCacheMaxSize) { this.metadataCacheMaxSize = metadataCacheMaxSize; }

    public long getMaxObjectSize() { return maxObjectSize; }
    public void setMaxObjectSize(long maxObjectSize) { this.maxObjectSize = maxObjectSize; }

    public long getMetadataIndexMaxEntries() { return metadataIndexMaxEntries; }
    public void setMetadataIndexMaxEntries(long metadataIndexMaxEntries) {
        this.metadataIndexMaxEntries = metadataIndexMaxEntries;
    }

    public int getImportStagingMaxAggregates() { return importStagingMaxAggregates; }
    public void setImportStagingMaxAggregates(int importStagingMaxAggregates) {
        this.importStagingMaxAggregates = importStagingMaxAggregates;
    }

    public int getImportStagingTimeoutMinutes() { return importStagingTimeoutMinutes; }
    public void setImportStagingTimeoutMinutes(int importStagingTimeoutMinutes) {
        this.importStagingTimeoutMinutes = importStagingTimeoutMinutes;
    }

    public boolean isStrictBucketCheck() { return strictBucketCheck; }
    public void setStrictBucketCheck(boolean strictBucketCheck) {
        this.strictBucketCheck = strictBucketCheck;
    }

    public boolean isReplicaSkipFsync() { return replicaSkipFsync; }
    public void setReplicaSkipFsync(boolean replicaSkipFsync) {
        this.replicaSkipFsync = replicaSkipFsync;
    }

    public long getTempSweepAgeMillis() { return tempSweepAgeMillis; }
    public void setTempSweepAgeMillis(long tempSweepAgeMillis) {
        this.tempSweepAgeMillis = tempSweepAgeMillis;
    }

    public long getTempGcIntervalMillis() { return tempGcIntervalMillis; }
    public void setTempGcIntervalMillis(long tempGcIntervalMillis) {
        this.tempGcIntervalMillis = tempGcIntervalMillis;
    }

    public boolean isSemiSyncEnabled() { return semiSyncEnabled; }
    public void setSemiSyncEnabled(boolean semiSyncEnabled) {
        this.semiSyncEnabled = semiSyncEnabled;
    }

    public long getSemiSyncTimeoutMs() { return semiSyncTimeoutMs; }
    public void setSemiSyncTimeoutMs(long semiSyncTimeoutMs) {
        this.semiSyncTimeoutMs = semiSyncTimeoutMs;
    }

    public boolean isSemiSyncFailClosed() { return semiSyncFailClosed; }
    public void setSemiSyncFailClosed(boolean semiSyncFailClosed) {
        this.semiSyncFailClosed = semiSyncFailClosed;
    }
}
