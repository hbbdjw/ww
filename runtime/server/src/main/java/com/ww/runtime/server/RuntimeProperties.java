package com.ww.runtime.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import com.ww.runtime.membership.MembershipConfig;
import com.ww.runtime.migration.MigrationConfig;
import com.ww.runtime.scheduler.SchedulerConfig;

/**
 * runtime-server 节点配置（前缀 {@code runtime}，record 构造绑定）。
 *
 * <p>覆盖优先级：环境变量（Spring 原生 relaxed binding，如 {@code RUNTIME_NODE_ID}
 * → {@code runtime.node.id}）&gt; application.yml &gt; 本类 {@code @DefaultValue} 默认值。</p>
 *
 * @param node                本节点标识与对外地址
 * @param store               持久化模式：{@code jdbc}（PostgreSQL，默认）或 {@code memory}（进程内，单测/演示）
 * @param heartbeatIntervalMs 心跳间隔（毫秒）
 * @param suspectAfterMisses  连续错过多少个心跳周期判 SUSPECTED（φ 暖窗期回退阈值）
 * @param deadAfterMisses     连续错过多少个心跳周期判 DEAD（φ 暖窗期回退阈值）
 * @param phiSuspectThreshold SUSPECTED 的 φ 怀疑度阈值（默认 3，约 0.1% 误判概率）
 * @param phiDeadThreshold    DEAD 的 φ 阈值（默认 8，误判概率约亿分之一）
 * @param deadConfirmations   φ 连续多少个检测周期超过死阈值才升级 DEAD（防抖，默认 2）
 * @param phiMaxSampleSize    φ 检测器心跳间隔滑动窗口容量（默认 200）
 * @param phiMinSamples       启用 φ 判定所需最少间隔样本数（暖窗保护，默认 3）
 * @param phiMinStdDeviationMs 心跳间隔标准差下限（毫秒，默认 100）：防稳态零方差令检测器过敏
 * @param phiAcceptablePauseMs 可接受暂停（毫秒，默认 0）：该窗口内静默不计入怀疑度（容忍 GC/调度停顿）
 * @param scheduler           调度器租约/调谐节拍
 * @param partitionCount      分区总数
 * @param replicationFactor   每分区副本数
 * @param minMoveRatio        触发扩容重平衡的最小移动比例
 * @param rebalanceCooldownMs 重平衡冷却时间（毫秒）
 * @param healingGracePeriodMs 治愈宽限期（毫秒）：节点被判 DEAD 后，无存活副本分区的
 *                            降级治愈迁移须等 DEAD 持续超过该期限才放行（押注节点重启）
 * @param suspicionVoteTtlMs  怀疑票有效期（毫秒，默认 12000）：超期未刷新不计入法定人数
 * @param suspicionMinObservers 法定人数下限（默认 1）：阈值=max(下限, floor((N-1)/2)+1)
 * @param suspicionQuorumEnabled 是否开启多数仲裁 DEAD（默认 true；本地 φ 判定
 *                            只投票不直写 DEAD，由 leader 按多数票唯一提升——
 *                            防止单观察者/单向网络故障误杀健康节点）
 * @param healingQuarantineEnabled P0-2 隔离治愈开关（默认 true）：无存活副本分区治愈宽限期
 *                            赌输后标记 DEGRADED 隔离（放置留盘押注重启、数据面拒绝服务），
 *                            替代旧的「空分区迁移」静默丢失路径；false 回退旧行为
 * @param migration           迁移引擎参数
 * @param capabilities        能力注册选项
 * @param log                 日志采集与实时推送配置
 * @param security            安全与会话配置
 * @param route               数据面路由视图模式与刷新周期
 * @param fence               本地写租约（Phase 2 fencing）参数
 */
@ConfigurationProperties(prefix = "runtime")
public record RuntimeProperties(
        @DefaultValue Node node,
        @DefaultValue("jdbc") String store,
        @DefaultValue("1000") long heartbeatIntervalMs,
        @DefaultValue("3") int suspectAfterMisses,
        @DefaultValue("6") int deadAfterMisses,
        @DefaultValue("3.0") double phiSuspectThreshold,
        @DefaultValue("8.0") double phiDeadThreshold,
        @DefaultValue("2") int deadConfirmations,
        @DefaultValue("200") int phiMaxSampleSize,
        @DefaultValue("3") int phiMinSamples,
        @DefaultValue("100") long phiMinStdDeviationMs,
        @DefaultValue("0") long phiAcceptablePauseMs,
        @DefaultValue Scheduler scheduler,
        @DefaultValue("4096") int partitionCount,
        @DefaultValue("1") int replicationFactor,
        @DefaultValue("0.10") double minMoveRatio,
        @DefaultValue("30000") long rebalanceCooldownMs,
        @DefaultValue("30000") long healingGracePeriodMs,
        @DefaultValue("12000") long suspicionVoteTtlMs,
        @DefaultValue("1") int suspicionMinObservers,
        @DefaultValue("true") boolean suspicionQuorumEnabled,
        @DefaultValue("true") boolean healingQuarantineEnabled,
        @DefaultValue Migration migration,
        @DefaultValue Capabilities capabilities,
        @DefaultValue Log log,
        @DefaultValue Security security,
        @DefaultValue Route route,
        @DefaultValue Fence fence) {

    public RuntimeProperties {
        if (route == null) {
            route = new Route("cache", 500L, 300_000L, 1_000L);
        }
        if (fence == null) {
            fence = new Fence(5_000L, 1_000L, 1_000L, 256);
        }
        if (node == null) {
            throw new IllegalArgumentException("runtime.node must not be null");
        }
        if (!"jdbc".equals(store) && !"memory".equals(store)) {
            throw new IllegalArgumentException("runtime.store must be 'jdbc' or 'memory', got: " + store);
        }
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("runtime.heartbeat-interval-ms must be > 0, got " + heartbeatIntervalMs);
        }
        if (suspectAfterMisses < 1) {
            throw new IllegalArgumentException("runtime.suspect-after-misses must be >= 1, got " + suspectAfterMisses);
        }
        if (deadAfterMisses < suspectAfterMisses) {
            throw new IllegalArgumentException("runtime.dead-after-misses (" + deadAfterMisses
                    + ") must be >= runtime.suspect-after-misses (" + suspectAfterMisses + ")");
        }
        if (!Double.isFinite(phiSuspectThreshold) || phiSuspectThreshold <= 0) {
            throw new IllegalArgumentException(
                    "runtime.phi-suspect-threshold must be positive finite, got " + phiSuspectThreshold);
        }
        if (!Double.isFinite(phiDeadThreshold) || phiDeadThreshold < phiSuspectThreshold) {
            throw new IllegalArgumentException("runtime.phi-dead-threshold must be finite and >= "
                    + "runtime.phi-suspect-threshold (" + phiSuspectThreshold + "), got " + phiDeadThreshold);
        }
        if (deadConfirmations < 1) {
            throw new IllegalArgumentException(
                    "runtime.dead-confirmations must be >= 1, got " + deadConfirmations);
        }
        if (phiMaxSampleSize < 1 || phiMinSamples < 1 || phiMinSamples > phiMaxSampleSize) {
            throw new IllegalArgumentException("runtime.phi sample window invalid: phi-min-samples ("
                    + phiMinSamples + ") must be in [1, phi-max-sample-size (" + phiMaxSampleSize + ")]");
        }
        if (phiMinStdDeviationMs <= 0) {
            throw new IllegalArgumentException(
                    "runtime.phi-min-std-deviation-ms must be > 0, got " + phiMinStdDeviationMs);
        }
        if (phiAcceptablePauseMs < 0) {
            throw new IllegalArgumentException(
                    "runtime.phi-acceptable-pause-ms must be >= 0, got " + phiAcceptablePauseMs);
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("runtime.partition-count must be > 0, got " + partitionCount);
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("runtime.replication-factor must be > 0, got " + replicationFactor);
        }
        if (healingGracePeriodMs < 0) {
            throw new IllegalArgumentException("runtime.healing-grace-period-ms must be >= 0, got "
                    + healingGracePeriodMs);
        }
        if (suspicionVoteTtlMs <= 0) {
            throw new IllegalArgumentException(
                    "runtime.suspicion-vote-ttl-ms must be > 0, got " + suspicionVoteTtlMs);
        }
        if (suspicionMinObservers < 1) {
            throw new IllegalArgumentException(
                    "runtime.suspicion-min-observers must be >= 1, got " + suspicionMinObservers);
        }
        if (!"cache".equals(route.mode()) && !"db".equals(route.mode())) {
            throw new IllegalArgumentException(
                    "runtime.route.mode must be 'cache' or 'db', got: " + route.mode());
        }
        if (fence.leaseRenewMs() >= fence.leaseTtlMs()) {
            throw new IllegalArgumentException(
                    "runtime.fence.lease-renew-ms must be < runtime.fence.lease-ttl-ms");
        }
        if (fence.leaseRenewMs() + fence.safetyMarginMs() >= fence.leaseTtlMs()) {
            throw new IllegalArgumentException(
                    "runtime.fence.lease-renew-ms + safety-margin-ms must be < lease-ttl-ms");
        }
    }

    /** 本节点标识与地址。 */
    public record Node(
            @DefaultValue("node-1") String id,
            @DefaultValue("localhost") String host,
            @DefaultValue("9100") int grpcPort,
            @DefaultValue("1.0") double weight,
            /** P1-9 故障域（机架/可用区）：副本放置优先跨域；空串 = 未配置。 */
            @DefaultValue("") String zone) {

        public Node {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("runtime.node.id must not be blank");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("runtime.node.host must not be blank");
            }
            if (grpcPort < 0 || grpcPort > 65535) {
                throw new IllegalArgumentException("runtime.node.grpc-port must be in [0, 65535], got " + grpcPort);
            }
            if (!Double.isFinite(weight) || weight <= 0) {
                throw new IllegalArgumentException("runtime.node.weight must be positive finite, got " + weight);
            }
            if (zone == null) {
                zone = "";
            }
        }
    }

    /** 调度器节拍（租约与调谐周期）。 */
    public record Scheduler(
            @DefaultValue("10000") long ttlMs,
            @DefaultValue("3000") long renewIntervalMs,
            @DefaultValue("2000") long reconcileIntervalMs) {
    }

    /** 迁移引擎参数。 */
    public record Migration(
            @DefaultValue("2") int maxConcurrent,
            @DefaultValue("67108864") long maxBytesPerSecond,
            @DefaultValue("4194304") int chunkSize,
            @DefaultValue("8") int ackWindow,
            /** P0-1：迁移期源分区停写冻结 TTL（毫秒，默认 5 分钟，批间续期；崩溃后最多停写该时长）。 */
            @DefaultValue("300000") long partitionFreezeTtlMs) {
    }

    /** 能力注册选项：mockDesired=启动时自动把本节点 MOCK 期望态写为 ACTIVE（供调度器拉起）。 */
    public record Capabilities(
            @DefaultValue("true") boolean mockDesired,
            @DefaultValue("false") boolean cacheDesired,
            @DefaultValue("false") boolean cacheAofEnabled,
            @DefaultValue("./data/cache") String cacheDataDir,
            /** CACHE 专属副本数（默认 1：cache 无复制器，rf&gt;1 会产生幽灵副本，见 SchedulerConfig）。 */
            @DefaultValue("1") int cacheReplicationFactor,
            @DefaultValue("false") boolean storageDesired,
            @DefaultValue("") String storageDrives,
            @DefaultValue("./data/storage") String storageDataDir,
            @DefaultValue("53687091200") long storageMaxObjectSize,
            /** AOF 重写双写缓冲字节上限（C12）：超限中止本次重写，不丢写不 OOM。 */
            @DefaultValue("268435456") long cacheAofRewriteBufferMaxBytes,
            /** 存储元数据索引容量上限（T3 止血）：超限桶列举降级目录扫描；0=不限。 */
            @DefaultValue("20000000") long storageMetadataIndexMaxEntries,
            /** 迁移导入暂存聚合并发数上限（T1）。 */
            @DefaultValue("1024") int storageImportStagingMaxAggregates,
            /** 迁移导入暂存聚合超时分钟数（T1，流中断无 fin 的泄漏清扫）。 */
            @DefaultValue("30") int storageImportStagingTimeoutMinutes,
            /** P0-1 半同步复制总开关（默认 true：至少一个副本确认后才应答，收敛主死丢写窗口）。 */
            @DefaultValue("true") boolean storageSemiSyncEnabled,
            /** P0-1 半同步等待副本确认超时（毫秒，默认 2000）。 */
            @DefaultValue("2000") long storageSemiSyncTimeoutMs,
            /** P0-1 半同步超时策略（默认 false=降级应答+at-risk 观测；true=fail-closed 抛 503）。 */
            @DefaultValue("false") boolean storageSemiSyncFailClosed,
            /** CACHE key 数上限（LRU 驱逐）；&lt;=0 走引擎默认 10 万。 */
            @DefaultValue("100000") int cacheMaxSize,
            /** CACHE 近似字节水位上限；&lt;=0 不启用（默认，不改变现网行为）。 */
            @DefaultValue("0") long cacheMaxMemoryBytes,
            /** CACHE 单 Stream 条目上限；&lt;=0 走引擎默认 100 万。 */
            @DefaultValue("1000000") long cacheStreamMaxLen,
            /** CACHE 全 store PEL 条目上限；&lt;=0 不限（默认 100 万，防 PEL 无界堆积）。 */
            @DefaultValue("1000000") long cachePelMaxEntries,
            /** CACHE Stream 数量上限；&lt;=0 不限（默认 1 万，防 stream 元数据无界）。 */
            @DefaultValue("10000") long cacheMaxStreams) {

        public Capabilities {
            if (cacheReplicationFactor <= 0) {
                throw new IllegalArgumentException("runtime.capabilities.cache-replication-factor"
                        + " must be > 0, got " + cacheReplicationFactor);
            }
            if (cacheMaxSize < 0) {
                throw new IllegalArgumentException("runtime.capabilities.cache-max-size must be >= 0, got "
                        + cacheMaxSize);
            }
            if (cacheMaxMemoryBytes < 0) {
                throw new IllegalArgumentException("runtime.capabilities.cache-max-memory-bytes must be >= 0, got "
                        + cacheMaxMemoryBytes);
            }
            if (storageSemiSyncTimeoutMs <= 0) {
                throw new IllegalArgumentException("runtime.capabilities.storage-semi-sync-timeout-ms"
                        + " must be > 0, got " + storageSemiSyncTimeoutMs);
            }
        }
    }

    /** 日志采集与实时推送：rt_log 落库开关与批量参数（SSE/gRPC 广播始终启用）。 */
    public record Log(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("200") int batchSize,
            @DefaultValue("1000") long flushIntervalMs,
            @DefaultValue("10000") int queueCapacity) {

        public Log {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("runtime.log.batch-size must be > 0, got " + batchSize);
            }
            if (flushIntervalMs <= 0) {
                throw new IllegalArgumentException("runtime.log.flush-interval-ms must be > 0, got " + flushIntervalMs);
            }
            if (queueCapacity <= 0) {
                throw new IllegalArgumentException("runtime.log.queue-capacity must be > 0, got " + queueCapacity);
            }
        }
    }

    /**
     * 数据面路由视图配置。
     *
     * @param mode              cache=本地缓存视图 + 本地租约 fencing（默认）；db=回退每次查库（Phase 1 回滚开关）
     * @param viewRefreshMs     放置修订号轮询周期
     * @param fullRefreshMs     全量重载兜底周期（覆盖混合版本/漏推进修订号）
     * @param directoryRefreshMs 节点目录/能力 serving 集合刷新周期
     */
    public record Route(
            @DefaultValue("cache") String mode,
            @DefaultValue("500") long viewRefreshMs,
            @DefaultValue("300000") long fullRefreshMs,
            @DefaultValue("1000") long directoryRefreshMs) {

        public Route {
            if (mode == null || (!"cache".equals(mode) && !"db".equals(mode))) {
                throw new IllegalArgumentException(
                        "runtime.route.mode must be 'cache' or 'db', got: " + mode);
            }
            if (viewRefreshMs <= 0) {
                throw new IllegalArgumentException(
                        "runtime.route.view-refresh-ms must be > 0, got " + viewRefreshMs);
            }
            if (fullRefreshMs < viewRefreshMs) {
                throw new IllegalArgumentException(
                        "runtime.route.full-refresh-ms must be >= view-refresh-ms");
            }
            if (directoryRefreshMs <= 0) {
                throw new IllegalArgumentException(
                        "runtime.route.directory-refresh-ms must be > 0, got " + directoryRefreshMs);
            }
        }
    }

    /**
     * 本地写租约配置（Phase 2）。
     *
     * @param leaseTtlMs     租约有效期（数据库时钟写入；本地有效期提前 safetyMargin 失效）
     * @param leaseRenewMs   后台续约周期（必须小于 TTL）
     * @param safetyMarginMs 本地租约提前失效量 + 新属主 validAfter 时钟偏移余量
     * @param leaseBatchSize 单次续约 SQL 的分区数上限
     */
    public record Fence(
            @DefaultValue("5000") long leaseTtlMs,
            @DefaultValue("1000") long leaseRenewMs,
            @DefaultValue("1000") long safetyMarginMs,
            @DefaultValue("256") int leaseBatchSize) {

        public Fence {
            if (leaseTtlMs <= 0) {
                throw new IllegalArgumentException(
                        "runtime.fence.lease-ttl-ms must be > 0, got " + leaseTtlMs);
            }
            if (leaseRenewMs <= 0 || leaseRenewMs >= leaseTtlMs) {
                throw new IllegalArgumentException(
                        "runtime.fence.lease-renew-ms must be in (0, lease-ttl-ms), got " + leaseRenewMs);
            }
            if (safetyMarginMs < 0 || leaseRenewMs + safetyMarginMs >= leaseTtlMs) {
                throw new IllegalArgumentException(
                        "runtime.fence.lease-renew-ms + safety-margin-ms must be < lease-ttl-ms");
            }
            if (leaseBatchSize <= 0) {
                throw new IllegalArgumentException(
                        "runtime.fence.lease-batch-size must be > 0, got " + leaseBatchSize);
            }
        }
    }

    /** 安全配置：会话与权限缓存的有效期（分钟）。 */
    public record Security(
            @DefaultValue("30") int sessionTimeoutMinutes,
            @DefaultValue("10") int permissionCacheMinutes) {

        public Security {
            if (sessionTimeoutMinutes <= 0) {
                throw new IllegalArgumentException("runtime.security.session-timeout-minutes must be > 0, got "
                        + sessionTimeoutMinutes);
            }
            if (permissionCacheMinutes <= 0) {
                throw new IllegalArgumentException("runtime.security.permission-cache-minutes must be > 0, got "
                        + permissionCacheMinutes);
            }
        }
    }

    /** 转换为 scheduler 模块配置（紧凑构造器内建校验）。 */
    public SchedulerConfig toSchedulerConfig() {
        return new SchedulerConfig(scheduler.ttlMs(), scheduler.renewIntervalMs(),
                scheduler.reconcileIntervalMs(), partitionCount, replicationFactor,
                capabilities.cacheReplicationFactor(), minMoveRatio, rebalanceCooldownMs,
                healingGracePeriodMs, suspicionVoteTtlMs, suspicionMinObservers,
                suspicionQuorumEnabled, healingQuarantineEnabled);
    }

    /** 转换为 membership 模块配置（紧凑构造器内建校验）。 */
    public MembershipConfig toMembershipConfig() {
        // 仲裁开启时本地 DEAD 不得直写注册表（由 leader 多数票提升）；关闭时保持传统语义。
        return MembershipConfig.of(heartbeatIntervalMs, suspectAfterMisses, deadAfterMisses,
                phiSuspectThreshold, phiDeadThreshold, deadConfirmations,
                phiMaxSampleSize, phiMinSamples, phiMinStdDeviationMs, phiAcceptablePauseMs,
                !suspicionQuorumEnabled);
    }

    /** 转换为 migration 模块配置（紧凑构造器内建校验）。 */
    public MigrationConfig toMigrationConfig() {
        return new MigrationConfig(migration.maxConcurrent(), migration.maxBytesPerSecond(),
                migration.chunkSize(), migration.ackWindow(), migration.partitionFreezeTtlMs());
    }
}
