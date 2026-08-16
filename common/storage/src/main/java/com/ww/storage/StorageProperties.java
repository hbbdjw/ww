package com.ww.storage;

import java.util.ArrayList;
import java.util.List;

import jakarta.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 对象存储配置，绑定 application.yml 中 storage.*。
 *
 * <p>集群拓扑复用 {@code cluster.*}（ClusterProperties），这里仅承载 storage 自身的本地磁盘、
 * Set 划分、加密开关、节点间 HTTP 通信端口偏移等配置。
 *
 * <p>单节点运行：cluster.enabled=false 或 nodes 仅 1 个时，所有读写直接走本地 Drive，
 * 不触发跨节点 HTTP 转发/复制，但仍严格遵守 read-after-write 一致性。
 */
@Component
@ConfigurationProperties(prefix = "storage")
public class StorageProperties {

    private static final Logger log = LoggerFactory.getLogger(StorageProperties.class);

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
     * 节点间 HTTP 端口推导偏移：peerHttpPort = cluster.node.port + httpPortOffset。
     * 默认 0；当 cluster.node.httpPort 显式配置时优先使用 httpPort。
     * 典型场景：cache TCP port=8701、HTTP server.port=8081，则 httpPortOffset=-620。
     */
    private int httpPortOffset = 0;

    /** 节点间通信认证 token，为空则不校验。 */
    private String authToken;

    /**
     * IO 直接内存缓冲区大小（字节）。用于 FileChannel 读写与 BufferedInputStream 缓冲。
     * 默认 64KB，建议 64KB~256KB。分配在堆外，不增加 GC 压力。
     */
    private int ioBufferSize = 64 * 1024;

    /**
     * 直接内存缓冲池上限（缓冲区个数）。每个缓冲区大小 = {@link #ioBufferSize}。
     * 默认 64 个，总堆外内存 = 64 * 64KB = 4MB。超出时归还即释放（由 Cleaner 回收）。
     */
    private int bufferPoolMaxSize = 64;

    /**
     * 对象元数据 LRU 缓存上限（条目数）。仅缓存本地主节点视角的元数据。
     * 默认 10,000 条，估算每条约 200B，总计约 2MB 堆内。0 表示禁用缓存。
     */
    private int metadataCacheMaxSize = 10_000;

    @PostConstruct
    public void printConfig() {
        log.info("storage 配置绑定: drives={}, drivesPerSet={}, rootDir={}, cryptoEnabled={}, httpPortOffset={}, " +
                        "ioBufferSize={}, bufferPoolMaxSize={}, metadataCacheMaxSize={}",
                drives, drivesPerSet, rootDir, cryptoEnabled, httpPortOffset,
                ioBufferSize, bufferPoolMaxSize, metadataCacheMaxSize);
    }

    public List<String> getDrives() { return drives; }
    public void setDrives(List<String> drives) { this.drives = drives; }

    public String getRootDir() { return rootDir; }
    public void setRootDir(String rootDir) { this.rootDir = rootDir; }

    public int getDrivesPerSet() { return drivesPerSet; }
    public void setDrivesPerSet(int drivesPerSet) { this.drivesPerSet = drivesPerSet; }

    public boolean isCryptoEnabled() { return cryptoEnabled; }
    public void setCryptoEnabled(boolean cryptoEnabled) { this.cryptoEnabled = cryptoEnabled; }

    public int getHttpPortOffset() { return httpPortOffset; }
    public void setHttpPortOffset(int httpPortOffset) { this.httpPortOffset = httpPortOffset; }

    public String getAuthToken() { return authToken; }
    public void setAuthToken(String authToken) { this.authToken = authToken; }

    public int getIoBufferSize() { return ioBufferSize; }
    public void setIoBufferSize(int ioBufferSize) { this.ioBufferSize = ioBufferSize; }

    public int getBufferPoolMaxSize() { return bufferPoolMaxSize; }
    public void setBufferPoolMaxSize(int bufferPoolMaxSize) { this.bufferPoolMaxSize = bufferPoolMaxSize; }

    public int getMetadataCacheMaxSize() { return metadataCacheMaxSize; }
    public void setMetadataCacheMaxSize(int metadataCacheMaxSize) { this.metadataCacheMaxSize = metadataCacheMaxSize; }
}
