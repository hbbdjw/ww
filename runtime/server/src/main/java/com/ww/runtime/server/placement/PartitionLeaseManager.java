package com.ww.runtime.server.placement;

import com.ww.runtime.common.CapabilityType;
import com.ww.runtime.common.Clock;
import com.ww.runtime.common.NodeId;
import com.ww.runtime.common.PartitionId;
import com.ww.runtime.placement.PlacementStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据面写租约管理器：后台批量续约本节点持有的 PRIMARY 分区。
 *
 * <p>写路径只查本地 {@link #isValid}，不访问数据库。租约续约是控制面后台动作：
 * 续约成功则本地有效期推进；数据库不可达时租约自然过期，写路径在 TTL 内继续
 * 服务、TTL 后 fail-closed（读不受影响）。</p>
 *
 * <p>切主时数据库把新属主的 {@code valid_after_ms} 设为“旧属主最后租约到期 +
 * 安全余量”，新属主在本地的 validAfter 到达前拒绝写入，旧属主在本地租约到期后
 * 停止写入，两者不会重叠。旧属主断网后无法续约，因此其租约上界由最后一次成功
 * 续约决定。</p>
 */
public final class PartitionLeaseManager implements CachedPlacementView.LeaseValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionLeaseManager.class);
    private static final long FAILURE_LOG_EVERY = 60L;

    private final PlacementStore placementStore;
    private final CachedPlacementView view;
    private final NodeId self;
    private final Clock clock;
    private final long ttlMs;
    private final long renewIntervalMs;
    private final long safetyMarginMs;
    private final int batchSize;
    private final boolean enabled;

    private final ConcurrentHashMap<LeaseKey, LocalLease> leases = new ConcurrentHashMap<>();
    private final AtomicLong renewFailures = new AtomicLong();
    private final Object renewLock = new Object();

    private volatile ScheduledExecutorService executor;
    private volatile boolean started;

    public PartitionLeaseManager(PlacementStore placementStore,
                                 CachedPlacementView view,
                                 NodeId self,
                                 Clock clock,
                                 long ttlMs,
                                 long renewIntervalMs,
                                 long safetyMarginMs,
                                 int batchSize,
                                 boolean enabled) {
        this.placementStore = Objects.requireNonNull(placementStore, "placementStore");
        this.view = Objects.requireNonNull(view, "view");
        this.self = Objects.requireNonNull(self, "self");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (ttlMs <= 0) {
            throw new IllegalArgumentException("lease ttl must be > 0, got " + ttlMs);
        }
        if (renewIntervalMs <= 0 || renewIntervalMs >= ttlMs) {
            throw new IllegalArgumentException("lease renew interval must be in (0, ttl), got "
                    + renewIntervalMs);
        }
        if (safetyMarginMs < 0 || renewIntervalMs + safetyMarginMs >= ttlMs) {
            throw new IllegalArgumentException(
                    "lease renew interval + safety margin must be < ttl");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("lease batch size must be > 0, got " + batchSize);
        }
        this.ttlMs = ttlMs;
        this.renewIntervalMs = renewIntervalMs;
        this.safetyMarginMs = safetyMarginMs;
        this.batchSize = batchSize;
        this.enabled = enabled;
    }

    @PostConstruct
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        if (!enabled) {
            LOG.info("local ownership leases disabled (route.mode != cache); data plane uses the durable view");
            return;
        }
        view.setLeaseValidator(this);
        view.setSnapshotListener(this::wakeUp);
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rt-partition-lease-renew");
            t.setDaemon(true);
            return t;
        });
        executor = exec;
        exec.scheduleWithFixedDelay(this::safeRenew, 0, renewIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("partition lease manager started: ttl={}ms renew={}ms safetyMargin={}ms batch={}",
                ttlMs, renewIntervalMs, safetyMarginMs, batchSize);
    }

    @PreDestroy
    public synchronized void stop() {
        if (!started) {
            return;
        }
        started = false;
        view.setSnapshotListener(null);
        ScheduledExecutorService exec = executor;
        executor = null;
        if (exec != null) {
            exec.shutdownNow();
        }
        if (enabled) {
            releaseNow();
            view.setLeaseValidator(null); // 停止后 fail-closed
        }
        leases.clear();
    }

    /** 快照变化后立即触发一次续约（非阻塞）。 */
    public void wakeUp() {
        ScheduledExecutorService exec = executor;
        if (exec != null && !exec.isShutdown()) {
            try {
                exec.execute(this::safeRenew);
            } catch (java.util.concurrent.RejectedExecutionException ignored) {
                // 停机窗口：忽略，租约按 TTL 自然过期
            }
        }
    }

    /** 立即续约一次（测试/管理面主动触发；线程安全）。 */
    public void renewNow() {
        synchronized (renewLock) {
            renewOnce();
        }
    }

    @Override
    public boolean isValid(CapabilityType capability, PartitionId partition, long epoch,
                           long nowMillis) {
        LocalLease lease = leases.get(new LeaseKey(capability, partition.value()));
        // 租约到期用单调时钟判定：墙钟回拨/步进不会延长或提前本地写权限。
        return lease != null && lease.epoch() == epoch
                && clock.monotonicMillis() < lease.expiresAtMonoMillis();
    }

    public int localLeaseCount() {
        return leases.size();
    }

    public long renewFailures() {
        return renewFailures.get();
    }

    private void safeRenew() {
        try {
            synchronized (renewLock) {
                renewOnce();
            }
        } catch (RuntimeException e) {
            long failures = renewFailures.incrementAndGet();
            if (failures == 1 || failures % FAILURE_LOG_EVERY == 0) {
                LOG.warn("partition lease renewal failed (leases expire naturally, failures={}): {}",
                        failures, e.getMessage());
            }
        }
    }

    private void renewOnce() {
        if (!enabled || !view.isReady()) {
            return;
        }
        List<CachedPlacementView.OwnedPartition> owned = view.ownedPartitions(self);
        Map<LeaseKey, Long> ownedEpochs = new HashMap<>();
        Map<LeaseGroup, List<Integer>> groups = new LinkedHashMap<>();
        for (CachedPlacementView.OwnedPartition partition : owned) {
            ownedEpochs.put(new LeaseKey(partition.capability(), partition.partition()),
                    partition.epoch());
            groups.computeIfAbsent(new LeaseGroup(partition.capability(), partition.epoch()),
                    ignored -> new ArrayList<>()).add(partition.partition());
        }

        for (Map.Entry<LeaseGroup, List<Integer>> entry : groups.entrySet()) {
            LeaseGroup group = entry.getKey();
            List<Integer> partitions = entry.getValue();
            for (int offset = 0; offset < partitions.size(); offset += batchSize) {
                List<Integer> batch = partitions.subList(offset,
                        Math.min(offset + batchSize, partitions.size()));
                try {
                    Map<Integer, Long> renewed = placementStore.renewLeases(
                            group.capability().name(), batch, self, group.epoch(), ttlMs);
                    // DB 租约 = 数据库 NOW() + ttl；本地用“响应时刻 + ttl - margin”的单调
                    // deadline，保证本地停写不晚于 DB 租约到期（与两台主机的墙钟无关）。
                    long localExpiry = clock.monotonicMillis() + ttlMs - safetyMarginMs;
                    for (Integer partition : renewed.keySet()) {
                        leases.put(new LeaseKey(group.capability(), partition),
                                new LocalLease(group.epoch(), localExpiry));
                    }
                } catch (RuntimeException e) {
                    // 保留既有租约到其本地到期时间；下一次续约或 TTL 到期收敛
                    LOG.debug("partition lease batch renewal failed for {} epoch {} ({} partitions): {}",
                            group.capability(), group.epoch(), batch.size(), e.getMessage());
                }
            }
        }

        // 已不再持有（或 epoch 已变化）的分区立即失效；仍持有的失败续约保留到本地到期
        leases.entrySet().removeIf(entry -> {
            Long currentEpoch = ownedEpochs.get(entry.getKey());
            return currentEpoch == null || currentEpoch != entry.getValue().epoch();
        });
    }

    private void releaseNow() {
        try {
            List<CachedPlacementView.OwnedPartition> owned = view.ownedPartitions(self);
            Map<LeaseGroup, List<Integer>> groups = new LinkedHashMap<>();
            for (CachedPlacementView.OwnedPartition partition : owned) {
                groups.computeIfAbsent(new LeaseGroup(partition.capability(), partition.epoch()),
                        ignored -> new ArrayList<>()).add(partition.partition());
            }
            for (Map.Entry<LeaseGroup, List<Integer>> entry : groups.entrySet()) {
                LeaseGroup group = entry.getKey();
                List<Integer> partitions = entry.getValue();
                for (int offset = 0; offset < partitions.size(); offset += batchSize) {
                    List<Integer> batch = partitions.subList(offset,
                            Math.min(offset + batchSize, partitions.size()));
                    try {
                        placementStore.releaseLeases(group.capability().name(), batch,
                                self);
                    } catch (RuntimeException e) {
                        LOG.debug("partition lease release failed for {} epoch {}: {}",
                                group.capability(), group.epoch(), e.getMessage());
                    }
                }
            }
        } catch (RuntimeException e) {
            LOG.debug("partition lease release skipped: {}", e.getMessage());
        }
    }

    private record LeaseKey(CapabilityType capability, int partition) {
    }

    private record LeaseGroup(CapabilityType capability, long epoch) {
    }

    private record LocalLease(long epoch, long expiresAtMonoMillis) {
    }
}
