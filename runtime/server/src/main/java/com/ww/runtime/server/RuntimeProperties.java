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
 * @param suspectAfterMisses  连续错过多少个心跳周期判 SUSPECTED
 * @param deadAfterMisses     连续错过多少个心跳周期判 DEAD
 * @param scheduler           调度器租约/调谐节拍
 * @param partitionCount      分区总数
 * @param replicationFactor   每分区副本数
 * @param minMoveRatio        触发扩容重平衡的最小移动比例
 * @param rebalanceCooldownMs 重平衡冷却时间（毫秒）
 * @param migration           迁移引擎参数
 * @param capabilities        能力注册选项
 * @param log                 日志采集与实时推送配置
 */
@ConfigurationProperties(prefix = "runtime")
public record RuntimeProperties(
        @DefaultValue Node node,
        @DefaultValue("jdbc") String store,
        @DefaultValue("1000") long heartbeatIntervalMs,
        @DefaultValue("3") int suspectAfterMisses,
        @DefaultValue("6") int deadAfterMisses,
        @DefaultValue Scheduler scheduler,
        @DefaultValue("4096") int partitionCount,
        @DefaultValue("1") int replicationFactor,
        @DefaultValue("0.10") double minMoveRatio,
        @DefaultValue("30000") long rebalanceCooldownMs,
        @DefaultValue Migration migration,
        @DefaultValue Capabilities capabilities,
        @DefaultValue Log log) {

    public RuntimeProperties {
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
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("runtime.partition-count must be > 0, got " + partitionCount);
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("runtime.replication-factor must be > 0, got " + replicationFactor);
        }
    }

    /** 本节点标识与地址。 */
    public record Node(
            @DefaultValue("node-1") String id,
            @DefaultValue("localhost") String host,
            @DefaultValue("9100") int grpcPort,
            @DefaultValue("1.0") double weight) {

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
            @DefaultValue("8") int ackWindow) {
    }

    /** 能力注册选项：mockDesired=启动时自动把本节点 MOCK 期望态写为 ACTIVE（供调度器拉起）。 */
    public record Capabilities(
            @DefaultValue("true") boolean mockDesired,
            @DefaultValue("false") boolean cacheDesired,
            @DefaultValue("false") boolean cacheAofEnabled,
            @DefaultValue("./data/cache") String cacheDataDir,
            @DefaultValue("false") boolean storageDesired,
            @DefaultValue("") String storageDrives,
            @DefaultValue("./data/storage") String storageDataDir) {
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

    /** 转换为 scheduler 模块配置（紧凑构造器内建校验）。 */
    public SchedulerConfig toSchedulerConfig() {
        return new SchedulerConfig(scheduler.ttlMs(), scheduler.renewIntervalMs(),
                scheduler.reconcileIntervalMs(), partitionCount, replicationFactor,
                minMoveRatio, rebalanceCooldownMs);
    }

    /** 转换为 membership 模块配置（紧凑构造器内建校验）。 */
    public MembershipConfig toMembershipConfig() {
        return new MembershipConfig(heartbeatIntervalMs, suspectAfterMisses, deadAfterMisses);
    }

    /** 转换为 migration 模块配置（紧凑构造器内建校验）。 */
    public MigrationConfig toMigrationConfig() {
        return new MigrationConfig(migration.maxConcurrent(), migration.maxBytesPerSecond(),
                migration.chunkSize(), migration.ackWindow());
    }
}
